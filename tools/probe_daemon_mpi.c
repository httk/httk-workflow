/* Site-only MPI/SHM probe. Build with mpicc -O2 -Wall -Wextra -o probe probe_daemon_mpi.c -lrt.
 * Run INSIDE the configured daemon MPI wrapper: httk workflow mpi run -- ./probe TOKEN [--spawn]
 * TOKEN must be a fresh alphanumeric string shared by all ranks.
 * --spawn requires independently confirmed spare capacity. It never writes to host home paths.
 * Exit 0: communication passed (and, if requested, explicit unsupported-spawn evidence).
 * Exit 1: communication failure or dynamic spawn succeeded. Exit 77: spawn refusal inconclusive.
 * Positive communication does not prove which MPI transport was selected: record transport logs.
 */
#include <mpi.h>
#include <ctype.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

int main(int argc, char **argv)
{
    MPI_Init(&argc, &argv);
    MPI_Comm parent = MPI_COMM_NULL;
    MPI_Comm_get_parent(&parent);
    if (parent != MPI_COMM_NULL) {
        fprintf(stderr, "FAIL: dynamically spawned child executed (pid %ld)\n", (long)getpid());
        MPI_Comm_disconnect(&parent);
        MPI_Finalize();
        return 1;
    }
    int rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    if (argc < 2 || strlen(argv[1]) < 8 || strlen(argv[1]) > 48) {
        if (rank == 0) fprintf(stderr, "Supply a fresh 8..48 character alphanumeric token\n");
        MPI_Abort(MPI_COMM_WORLD, 2);
    }
    for (const char *p = argv[1]; *p; ++p) {
        if (!isalnum((unsigned char)*p)) MPI_Abort(MPI_COMM_WORLD, 2);
    }
    long long rank_value = rank, sum = 0, expected = (long long)size * (size - 1) / 2;
    MPI_Allreduce(&rank_value, &sum, 1, MPI_LONG_LONG, MPI_SUM, MPI_COMM_WORLD);
    int fail = sum != expected;
    int received = -1;
    MPI_Sendrecv(&rank, 1, MPI_INT, (rank + 1) % size, 0,
                 &received, 1, MPI_INT, (rank + size - 1) % size, 0,
                 MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    fail |= received != (rank + size - 1) % size;

    MPI_Comm local;
    MPI_Comm_split_type(MPI_COMM_WORLD, MPI_COMM_TYPE_SHARED, rank, MPI_INFO_NULL, &local);
    int local_rank, local_size;
    MPI_Comm_rank(local, &local_rank);
    MPI_Comm_size(local, &local_size);
    char name[80];
    snprintf(name, sizeof name, "/httk-mpi-probe-%s", argv[1]);
    size_t bytes = (size_t)local_size * sizeof(int);
    int fd = -1;
    if (local_rank == 0) {
        fd = shm_open(name, O_RDWR | O_CREAT | O_EXCL, 0600);
        if (fd < 0 || ftruncate(fd, (off_t)bytes) != 0) {
            perror("shared-memory creation");
            MPI_Abort(MPI_COMM_WORLD, 1);
        }
    }
    MPI_Barrier(local);
    if (local_rank != 0) fd = shm_open(name, O_RDWR, 0600);
    if (fd < 0) {
        perror("peer shared-memory open");
        MPI_Abort(MPI_COMM_WORLD, 1);
    }
    volatile int *slots = mmap(NULL, bytes, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if ((void *)slots == MAP_FAILED) MPI_Abort(MPI_COMM_WORLD, 1);
    slots[local_rank] = local_rank + 1;
    MPI_Barrier(local);
    for (int i = 0; i < local_size; ++i) fail |= slots[i] != i + 1;
    MPI_Barrier(local);
    munmap((void *)slots, bytes);
    close(fd);
    if (local_rank == 0) shm_unlink(name);
    int peers = local_size > 1, peer_nodes;
    MPI_Allreduce(&peers, &peer_nodes, 1, MPI_INT, MPI_SUM, MPI_COMM_WORLD);
    int total_fail;
    MPI_Allreduce(&fail, &total_fail, 1, MPI_INT, MPI_MAX, MPI_COMM_WORLD);
    int verdict = total_fail ? 1 : (peer_nodes ? 0 : 77);
    if (rank == 0) {
        printf("%s: collectives, ring exchange and allocation POSIX shared memory; ranks=%d\n",
               total_fail ? "FAIL" : "PASS", size);
        if (!peer_nodes) printf("INCONCLUSIVE: no node had multiple ranks to test shared-memory peers\n");
    }
    if (argc > 2 && strcmp(argv[2], "--spawn") == 0 && rank == 0) {
        MPI_Comm_set_errhandler(MPI_COMM_SELF, MPI_ERRORS_RETURN);
        MPI_Comm child = MPI_COMM_NULL;
        int child_error = MPI_SUCCESS;
        char self[4096];
        ssize_t length = readlink("/proc/self/exe", self, sizeof self - 1);
        if (length <= 0 || length >= (ssize_t)sizeof self - 1) MPI_Abort(MPI_COMM_WORLD, 2);
        self[length] = '\0';
        int result = MPI_Comm_spawn(self, MPI_ARGV_NULL, 1, MPI_INFO_NULL, 0,
                                    MPI_COMM_SELF, &child, &child_error);
        if (result == MPI_SUCCESS) {
            printf("FAIL: dynamic MPI spawn succeeded; do not accept this configuration\n");
            verdict = 1;
            if (child != MPI_COMM_NULL) MPI_Comm_disconnect(&child);
        } else {
            char reason[MPI_MAX_ERROR_STRING + 1];
            int count;
            MPI_Error_string(result, reason, &count);
            reason[count] = '\0';
            printf("Spawn rc=%d, child rc=%d: %s\n", result, child_error, reason);
            for (int i = 0; i < count; ++i) reason[i] = (char)tolower((unsigned char)reason[i]);
            if (strstr(reason, "not supported") || strstr(reason, "unsupported")) {
                printf("REJECTED: explicit unsupported-operation evidence; other escape routes still require checks\n");
            } else {
                printf("INCONCLUSIVE: generic spawn/configuration/resource error is not a containment pass\n");
                if (!verdict) verdict = 77;
            }
        }
    }
    MPI_Bcast(&verdict, 1, MPI_INT, 0, MPI_COMM_WORLD);
    MPI_Comm_free(&local);
    MPI_Finalize();
    return verdict;
}
