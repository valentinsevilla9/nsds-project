/*
 * district_simulation.c — Componente 3 (MPI), Proyecto #1 Smart Power Grid
 *
 * Segunda implementacion del Componente 3, en paralelo a la version Akka
 * (component3-simulation), pensada para comparar de verdad estrategias de
 * distribucion entre procesos: la topologia logica (que rank pertenece a
 * que distrito) no cambia nunca; lo que cambia entre estrategias es el
 * mapping rank->host, controlado por el hostfile de mpirun
 * (ver run-by-district.sh / run-round-robin.sh).
 *
 * Un rank = un nodo de la grid (producer/consumer/accumulator), con los
 * mismos rangos de generacion que NodeActor/DistrictActor en la version
 * Akka. Dentro de cada distrito:
 *   - Se intercambia el valor de energia con los vecinos del anillo
 *     (MPI_Isend/MPI_Irecv), modelando el "exchange energy with
 *     neighboring nodes" del enunciado.
 *   - Se agrega el balance del distrito con MPI_Reduce (root = el rank
 *     acumulador), que actualiza su estado de carga.
 *   - MPI_Barrier sincroniza los pasos de simulacion entre todos los ranks.
 * Al final, MPI_Gather recoge a rank 0 el tiempo total pasado en
 * MPI_Reduce por cada rank, para poder comparar el overhead de
 * comunicacion real entre estrategias.
 *
 * Uso: mpirun -np <N> ./district_simulation [nodesPerDistrict] [simSteps]
 */
#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>

typedef enum { PRODUCER, CONSUMER, ACCUMULATOR } NodeType;

static const char *type_name(NodeType t) {
    switch (t) {
        case PRODUCER: return "PRODUCER";
        case CONSUMER: return "CONSUMER";
        default:       return "ACCUMULATOR";
    }
}

static NodeType node_type_for(int district_rank, int district_size) {
    if (district_rank == district_size - 1) return ACCUMULATOR;
    if (district_rank < (int) (district_size * 0.6)) return PRODUCER;
    return CONSUMER;
}

static double generate_value(NodeType type, unsigned int *seed) {
    double r = (double) rand_r(seed) / RAND_MAX;
    switch (type) {
        case PRODUCER:    return 10.0 + r * 90.0;
        case CONSUMER:    return -(5.0 + r * 45.0);
        default:          return (r * 2.0 - 1.0) * 20.0;
    }
}

int main(int argc, char **argv) {
    MPI_Init(&argc, &argv);

    int world_rank, world_size;
    MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);

    int nodes_per_district = argc > 1 ? atoi(argv[1]) : 4;
    int sim_steps = argc > 2 ? atoi(argv[2]) : 10;

    if (world_rank == 0 && world_size % nodes_per_district != 0) {
        fprintf(stderr,
            "AVISO: numero de procesos (%d) no es multiplo de nodesPerDistrict (%d); "
            "el ultimo distrito quedara incompleto\n", world_size, nodes_per_district);
    }

    int district_id = world_rank / nodes_per_district;

    MPI_Comm district_comm;
    MPI_Comm_split(MPI_COMM_WORLD, district_id, world_rank, &district_comm);

    int district_size, district_rank;
    MPI_Comm_size(district_comm, &district_size);
    MPI_Comm_rank(district_comm, &district_rank);

    NodeType type = node_type_for(district_rank, district_size);
    int accumulator_rank = district_size - 1;

    char proc_name[MPI_MAX_PROCESSOR_NAME];
    int name_len;
    MPI_Get_processor_name(proc_name, &name_len);

    unsigned int seed = (unsigned int) (world_rank * 7919u + 1u);
    double accumulator_charge = 0.0;
    double total_reduce_time_ms = 0.0;

    int next = (district_rank + 1) % district_size;
    int prev = (district_rank - 1 + district_size) % district_size;

    for (int step = 0; step < sim_steps; step++) {
        double value = generate_value(type, &seed);
        printf("[distrito %d] rank %d (%s) en %s: paso=%d valor=%.2f kW\n",
               district_id, world_rank, type_name(type), proc_name, step, value);
        fflush(stdout);

        double neighbor_prev, neighbor_next;
        MPI_Request reqs[4];
        MPI_Irecv(&neighbor_prev, 1, MPI_DOUBLE, prev, step, district_comm, &reqs[0]);
        MPI_Irecv(&neighbor_next, 1, MPI_DOUBLE, next, step, district_comm, &reqs[1]);
        MPI_Isend(&value, 1, MPI_DOUBLE, next, step, district_comm, &reqs[2]);
        MPI_Isend(&value, 1, MPI_DOUBLE, prev, step, district_comm, &reqs[3]);
        MPI_Waitall(4, reqs, MPI_STATUSES_IGNORE);

        double district_balance = 0.0;
        double t0 = MPI_Wtime();
        MPI_Reduce(&value, &district_balance, 1, MPI_DOUBLE, MPI_SUM,
                   accumulator_rank, district_comm);
        total_reduce_time_ms += (MPI_Wtime() - t0) * 1000.0;

        if (district_rank == accumulator_rank) {
            accumulator_charge += district_balance / 60.0;
            if (accumulator_charge < 0) accumulator_charge = 0;
            printf("[distrito %d] BALANCE paso=%d balance=%.2f kW acumulador=%.4f kWh "
                   "vecinos(prev=%.2f,next=%.2f)\n",
                   district_id, step, district_balance, accumulator_charge,
                   neighbor_prev, neighbor_next);
            fflush(stdout);
        }

        MPI_Barrier(MPI_COMM_WORLD);
    }

    double *all_times = NULL;
    if (world_rank == 0) all_times = malloc(sizeof(double) * (size_t) world_size);
    MPI_Gather(&total_reduce_time_ms, 1, MPI_DOUBLE, all_times, 1, MPI_DOUBLE, 0, MPI_COMM_WORLD);

    if (world_rank == 0) {
        printf("\n=== Resumen: tiempo total en MPI_Reduce por rank (ms) ===\n");
        for (int r = 0; r < world_size; r++)
            printf("  rank %d: %.4f ms\n", r, all_times[r]);
        free(all_times);
    }

    MPI_Comm_free(&district_comm);
    MPI_Finalize();
    return 0;
}
