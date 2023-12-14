#include "err.h"
#include "utils.h"
#include <pthread.h>
#include <semaphore.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#define MAX_N_TASKS 4096
#define MAX_COMMAND_LENGTH 512
#define MAX_LINE_LENGTH 1024

int task_id = 0;

struct Lock {
    pthread_mutex_t mutex;
    pthread_cond_t end_cond, main_cond;
    sem_t wait_for_message;
    int waiting_for_end;
    bool main_working;
};
typedef struct Lock Lock;

void Lock_init(Lock *lock) {
    ASSERT_ZERO(pthread_mutex_init(&lock->mutex, NULL));
    ASSERT_ZERO(pthread_cond_init(&lock->end_cond, NULL));
    ASSERT_ZERO(pthread_cond_init(&lock->main_cond, NULL));
    ASSERT_SYS_OK(sem_init(&lock->wait_for_message, 0, 0));
    lock->waiting_for_end = 0;
    lock->main_working = false;
}

void Lock_destroy(Lock *lock) {
    ASSERT_ZERO(pthread_mutex_destroy(&lock->mutex));
    ASSERT_ZERO(pthread_cond_destroy(&lock->end_cond));
    ASSERT_ZERO(pthread_cond_destroy(&lock->main_cond));
    ASSERT_SYS_OK(sem_destroy(&lock->wait_for_message));
}

Lock *lock;

struct Listener {
    int desc;
    char line[MAX_LINE_LENGTH];
    pthread_mutex_t mutex;
};
typedef struct Listener Listener;

void init_listener(Listener *l) {
    ASSERT_ZERO(pthread_mutex_init(&l->mutex, NULL));
    l->line[0] = '\0';
}

struct Task {
    char **arguments;
    int task_id;
    pid_t task_pid;
    Listener *out, *err;
};
typedef struct Task Task;

void init_task(Task *t, char **arguments) {
    t->arguments = arguments;
    t->task_id = task_id;
    t->out = malloc(sizeof(Listener));
    t->err = malloc(sizeof(Listener));
    init_listener(t->out);
    init_listener(t->err);
}

void destroy_task(Task *t) {
    free_split_string(t->arguments);
    free(t->out);
    free(t->err);
}

Task *tasks[MAX_N_TASKS];
pthread_t run_threads[MAX_N_TASKS];

void *listen_main(void *data) {
    Listener *l = data;
    char *buffer = malloc(MAX_LINE_LENGTH * sizeof(char));
    FILE *file = fdopen(l->desc, "r");

    while (read_line(buffer, MAX_LINE_LENGTH, file)) {
        ASSERT_ZERO(pthread_mutex_lock(&l->mutex));
        size_t d = strlen(buffer) + 1;
        memcpy(l->line, buffer, d);
        ASSERT_ZERO(pthread_mutex_unlock(&l->mutex));
    }

    ASSERT_SYS_OK(close(l->desc));

    free(buffer);
    return NULL;
}

void before_end() {
    ASSERT_ZERO(pthread_mutex_lock(&lock->mutex));
    lock->waiting_for_end++;
    while (lock->main_working) {
        ASSERT_ZERO(pthread_cond_wait(&lock->end_cond, &lock->mutex));
    }
    ASSERT_ZERO(pthread_mutex_unlock(&lock->mutex));
}

void after_end() {
    ASSERT_ZERO(pthread_mutex_lock(&lock->mutex));
    lock->waiting_for_end--;
    if (lock->waiting_for_end) {
        ASSERT_ZERO(pthread_cond_signal(&lock->end_cond));
    }
    else {
        ASSERT_ZERO(pthread_cond_signal(&lock->main_cond));
    }
    ASSERT_ZERO(pthread_mutex_unlock(&lock->mutex));
}

void before_command() {
    ASSERT_ZERO(pthread_mutex_lock(&lock->mutex));
    while (lock->waiting_for_end > 0) {
        ASSERT_ZERO(pthread_cond_wait(&lock->main_cond, &lock->mutex));
    }
    lock->main_working = true;
    ASSERT_ZERO(pthread_mutex_unlock(&lock->mutex));
}

void after_command() {
    ASSERT_ZERO(pthread_mutex_lock(&lock->mutex));
    lock->main_working = false;
    if (lock->waiting_for_end > 0) {
        ASSERT_ZERO(pthread_cond_signal(&lock->end_cond));
    }
    ASSERT_ZERO(pthread_mutex_unlock(&lock->mutex));
}

void *run_main(void *data) {
    Task *task = data;
    int pipes[2][2];

    for (int i = 0; i < 2; ++i) {
        ASSERT_SYS_OK(pipe(pipes[i]));
    }

    task->out->desc = pipes[0][0];
    task->err->desc = pipes[1][0];
    set_close_on_exec(task->out->desc, true);
    set_close_on_exec(task->err->desc, true);

    pid_t pid;
    pid = fork();
    ASSERT_SYS_OK(pid);
    if (pid == 0) {
        for (int i = 0; i < 2; ++i) {
            int end = i ? STDERR_FILENO : STDOUT_FILENO;
            ASSERT_SYS_OK(close(pipes[i][0]));
            ASSERT_SYS_OK(dup2(pipes[i][1], end));
            ASSERT_SYS_OK(close(pipes[i][1]));
        }
        execvp(task->arguments[1], &task->arguments[1]);
    }

    task->task_pid = pid;
    for (int i = 0; i < 2; ++i) {
        ASSERT_SYS_OK(close(pipes[i][1]));
    }

    pthread_t listener_out, listener_err;
    ASSERT_ZERO(pthread_create(&listener_out, NULL, listen_main, task->out));
    ASSERT_ZERO(pthread_create(&listener_err, NULL, listen_main, task->err));

    printf("Task %d started: pid %d.\n", task->task_id, pid);
    ASSERT_SYS_OK(sem_post(&lock->wait_for_message));

    ASSERT_ZERO(pthread_join(listener_out, NULL));
    ASSERT_ZERO(pthread_join(listener_err, NULL));

    before_end();
    int status;
    waitpid(pid, &status, 0);
    if (WIFSIGNALED(status)) {
        printf("Task %d ended: signalled.\n", task->task_id);
    }
    else {
        printf("Task %d ended: status %d.\n", task->task_id, WEXITSTATUS(status));
    }
    after_end();

    return NULL;
}

void run(char **arguments) {
    Task *task = malloc(sizeof(Task));
    init_task(task, arguments);
    tasks[task_id] = task;
    ASSERT_ZERO(pthread_create(&run_threads[task_id], NULL, run_main, task));
    ++task_id;
}

void kill_all() {
    for (int i = 0; i < task_id; ++i) {
        kill(tasks[i]->task_pid, SIGKILL);
    }
}

int main() {
    lock = malloc(sizeof(Lock));
    Lock_init(lock);
    char *buffer = malloc(MAX_COMMAND_LENGTH * sizeof(char));
    while (read_line(buffer, MAX_COMMAND_LENGTH, stdin)) {
        before_command();
        char **arguments = split_string(buffer);
        int option = get_command(arguments[0]);
        if (option == RUN) {
            run(arguments);
            ASSERT_SYS_OK(sem_wait(&lock->wait_for_message));
        }
        else if (option == OUT) {
            int t = atoi(arguments[1]);
            Task *task = tasks[t];
            ASSERT_ZERO(pthread_mutex_lock(&task->out->mutex));
            printf("Task %d stdout: '%s'.\n", t, task->out->line);
            ASSERT_ZERO(pthread_mutex_unlock(&task->out->mutex));
        }
        else if (option == ERR) {
            int t = atoi(arguments[1]);
            Task *task = tasks[t];
            ASSERT_ZERO(pthread_mutex_lock(&task->err->mutex));
            printf("Task %d stderr: '%s'.\n", t, task->err->line);
            ASSERT_ZERO(pthread_mutex_unlock(&task->err->mutex));
        }
        else if (option == KILL) {
            int t = atoi(arguments[1]);
            kill(tasks[t]->task_pid, SIGINT);
        }
        else if (option == SLEEP) {
            int n = atoi(arguments[1]);
            usleep(n * 1000);
        }
        else if (option == QUIT) {
            kill_all();
            after_command();
            free_split_string(arguments);
            break;
        }
        else if (option != EMPTY) {
            fatal("Wrong command.\n");
        }
        after_command();
        if (option != RUN) {
            free_split_string(arguments);
        }
    }
    kill_all();
    for (int i = 0; i < task_id; ++i) {
        ASSERT_ZERO(pthread_join(run_threads[i], NULL));
        destroy_task(tasks[i]);
        free(tasks[i]);
    }
    free(buffer);
    Lock_destroy(lock);

    return 0;
}