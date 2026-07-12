/*
 * Componente 3 (MPI)
 *
 * La idea de fondo: un rank = un nodo de la grid. Los ranks se agrupan en
 * distritos con MPI_Comm_split, y esa topologia no cambia nunca pase lo que
 * pase. Lo unico que cambia entre las dos estrategias que comparamos es el
 * mapping rank->host, y eso lo decide el hostfile con el que lanzamos mpirun
 * (run-by-district.sh vs run-round-robin.sh).
 *
 * Dentro de cada distrito, en cada paso: cada nodo genera su valor de
 * energia, se lo pasa a sus dos vecinos del anillo, y luego se suma todo
 * con MPI_Reduce en el rank que hace de acumulador, que es quien
 * actualiza su carga.
 *
 * Uso: mpirun -np <N> ./district_simulation [nodesPerDistrict] [simSteps]
 */
#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>

typedef enum { PRODUCER, CONSUMER, ACCUMULATOR } NodeType;

static const char *type_name(NodeType t) {
  switch (t) {
  case PRODUCER:
    return "PRODUCER";
  case CONSUMER:
    return "CONSUMER";
  default:
    return "ACCUMULATOR";
  }
}

// el ultimo rank de cada distrito siempre es el acumulador, el 60%
// de los primeros son productores, el resto consumidores
static NodeType node_type_for(int district_rank, int district_size) {
  if (district_rank == district_size - 1)
    return ACCUMULATOR;
  if (district_rank < (int)(district_size * 0.6))
    return PRODUCER;
  return CONSUMER;
}

// mismos rangos que usan MeasurementService (Kafka) y NodeActor lo tenia
// tambien la version Akka, asi que los tres modelos generan valores comparables entre si
static double generate_value(NodeType type, unsigned int *seed) {
  double r = (double)rand_r(seed) / RAND_MAX;
  switch (type) {
  case PRODUCER:
    return 10.0 + r * 90.0;
  case CONSUMER:
    return -(5.0 + r * 45.0);
  default:
    return (r * 2.0 - 1.0) * 20.0;
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
            "WARNING: number of processes (%d) is not a multiple of "
            "nodesPerDistrict (%d); "
            "the last district will be incomplete\n",
            world_size, nodes_per_district);
  }

  int district_id = world_rank / nodes_per_district;

  // aqui es donde se decide que ranks forman cada distrito - esto no
  // cambia jamas entre estrategias, solo cambia donde vive fisicamente
  // cada rank
  MPI_Comm district_comm;
  MPI_Comm_split(MPI_COMM_WORLD, district_id, world_rank, &district_comm);

  int district_size, district_rank;
  MPI_Comm_size(district_comm, &district_size);
  MPI_Comm_rank(district_comm, &district_rank);

  NodeType type = node_type_for(district_rank, district_size);
  int accumulator_rank = district_size - 1;

  // esto es solo para comprobar en los logs que la estrategia esta
  // colocando los ranks donde toca (mismo host o repartidos)
  char proc_name[MPI_MAX_PROCESSOR_NAME];
  int name_len;
  MPI_Get_processor_name(proc_name, &name_len);

  unsigned int seed = (unsigned int)(world_rank * 7919u + 1u);
  double accumulator_charge = 0.0;
  double total_reduce_time_ms = 0.0;

  int next = (district_rank + 1) % district_size;
  int prev = (district_rank - 1 + district_size) % district_size;

  for (int step = 0; step < sim_steps; step++) {
    double value = generate_value(type, &seed);
    printf("[district %d] rank %d (%s) on %s: step=%d value=%.2f kW\n",
           district_id, world_rank, type_name(type), proc_name, step, value);
    fflush(stdout);

    // anillo con los dos vecinos: mandamos y recibimos a la vez sin
    // bloquear, y esperamos a que las 4 operaciones terminen
    double neighbor_prev, neighbor_next;
    MPI_Request reqs[4];
    MPI_Irecv(&neighbor_prev, 1, MPI_DOUBLE, prev, step, district_comm,
              &reqs[0]);
    MPI_Irecv(&neighbor_next, 1, MPI_DOUBLE, next, step, district_comm,
              &reqs[1]);
    MPI_Isend(&value, 1, MPI_DOUBLE, next, step, district_comm, &reqs[2]);
    MPI_Isend(&value, 1, MPI_DOUBLE, prev, step, district_comm, &reqs[3]);
    MPI_Waitall(4, reqs, MPI_STATUSES_IGNORE);

    // esto es lo que cronometramos: cuanto tarda juntar el balance
    // del distrito. Si el distrito esta repartido entre hosts (round-
    // robin), este Reduce cruza la red y tarda mas - eso es justo lo
    // que queremos poder comparar entre estrategias
    double district_balance = 0.0;
    double t0 = MPI_Wtime();
    MPI_Reduce(&value, &district_balance, 1, MPI_DOUBLE, MPI_SUM,
               accumulator_rank, district_comm);
    total_reduce_time_ms += (MPI_Wtime() - t0) * 1000.0;

    if (district_rank == accumulator_rank) {
      accumulator_charge += district_balance / 60.0;
      if (accumulator_charge < 0)
        accumulator_charge = 0; // la carga nunca baja de 0
      printf(
          "[district %d] BALANCE step=%d balance=%.2f kW accumulator=%.4f kWh "
          "neighbors(prev=%.2f,next=%.2f)\n",
          district_id, step, district_balance, accumulator_charge,
          neighbor_prev, neighbor_next);
      fflush(stdout);
    }

    MPI_Barrier(
        MPI_COMM_WORLD); // todos esperan antes de pasar al siguiente paso
  }

  // al acabar, cada rank manda cuanto tiempo total ha pasado
  // comunicando, y rank 0 lo imprime todo junto para poder comparar
  double *all_times = NULL;
  if (world_rank == 0)
    all_times = malloc(sizeof(double) * (size_t)world_size);
  MPI_Gather(&total_reduce_time_ms, 1, MPI_DOUBLE, all_times, 1, MPI_DOUBLE, 0,
             MPI_COMM_WORLD);

  if (world_rank == 0) {
    printf("\n=== Summary: total time in MPI_Reduce per rank (ms) ===\n");
    for (int r = 0; r < world_size; r++)
      printf("  rank %d: %.4f ms\n", r, all_times[r]);
    free(all_times);
  }

  MPI_Comm_free(&district_comm);
  MPI_Finalize();
  return 0;
}
