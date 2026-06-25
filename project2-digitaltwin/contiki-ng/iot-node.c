/*
 * iot-node.c — Nodo IoT para el Digital Twin (Proyecto #2)
 *
 * Basado en el udp-client.c del curso (misma estructura, mismas APIs).
 *
 * Funcionalidades añadidas respecto al ejemplo del profesor:
 *   a) Reporta cambios de padre RPL via serie: "PARENT_CHANGE:<newParentId>"
 *      Node-RED lee la consola serie y notifica al actor Akka correspondiente.
 *   b) Envía tráfico periódico dummy al root RPL (igual que el ejemplo).
 *   c) Escucha comandos UDP del root para cambiar el período T.
 *   d) El crash se simula deteniendo el proceso en Cooja (coherente con
 *      crash semantics — el proceso simplemente deja de ejecutarse).
 */

#include "contiki.h"
#include "net/routing/routing.h"
#include "net/routing/rpl-classic/rpl.h"
#include "net/routing/rpl-classic/rpl-private.h"
#include "random.h"
#include "net/netstack.h"
#include "net/ipv6/simple-udp.h"
#include "sys/log.h"

#define LOG_MODULE "App"
#define LOG_LEVEL LOG_LEVEL_INFO

#define UDP_CLIENT_PORT  8765
#define UDP_SERVER_PORT  5678

/* Período por defecto: 4 segundos (configurable via UDP) */
#define DEFAULT_SEND_INTERVAL (4 * CLOCK_SECOND)

static struct simple_udp_connection udp_conn;
static clock_time_t send_interval = DEFAULT_SEND_INTERVAL;
static uip_ipaddr_t last_parent;
static int has_last_parent = 0;
static unsigned seq_num = 0;

/*---------------------------------------------------------------------------*/
PROCESS(iot_node_process, "IoT Node Digital Twin");
AUTOSTART_PROCESSES(&iot_node_process);
/*---------------------------------------------------------------------------*/

/*
 * Detecta cambio de padre en el árbol RPL y lo reporta por la consola serie.
 * Node-RED lee estos mensajes y notifica al actor Akka correspondiente.
 * Requisito a) del enunciado.
 */
static void
check_parent_change(void)
{
  rpl_dag_t *dag = rpl_get_any_dag();
  if(dag == NULL || dag->preferred_parent == NULL) return;

  uip_ipaddr_t *current_parent = rpl_parent_get_ipaddr(dag->preferred_parent);
  if(current_parent == NULL) return;

  if(!has_last_parent ||
     !uip_ipaddr_cmp(current_parent, &last_parent)) {
    /* Nuevo padre detectado — reportar por serie */
    LOG_INFO("PARENT_CHANGE:");
    LOG_INFO_6ADDR(current_parent);
    LOG_INFO_("\n");
    uip_ipaddr_copy(&last_parent, current_parent);
    has_last_parent = 1;
  }
}

/*---------------------------------------------------------------------------*/
/*
 * Callback UDP: recibe comandos del root.
 * Formato: "PERIOD:<ms>" para cambiar el período de envío T.
 * Requisito c) del enunciado: Node-RED propaga cambios de T al nodo IoT.
 */
static void
udp_rx_callback(struct simple_udp_connection *c,
                const uip_ipaddr_t *sender_addr,
                uint16_t sender_port,
                const uip_ipaddr_t *receiver_addr,
                uint16_t receiver_port,
                const uint8_t *data,
                uint16_t datalen)
{
  /* Comando de cambio de período: "PERIOD:<segundos>" */
  if(datalen > 7 && data[0] == 'P' && data[1] == 'E' &&
     data[2] == 'R' && data[3] == 'I' && data[4] == 'O' &&
     data[5] == 'D' && data[6] == ':') {
    unsigned new_period_s = 0;
    unsigned i;
    for(i = 7; i < datalen && data[i] >= '0' && data[i] <= '9'; i++) {
      new_period_s = new_period_s * 10 + (data[i] - '0');
    }
    if(new_period_s > 0) {
      send_interval = new_period_s * CLOCK_SECOND;
      LOG_INFO("PERIOD_UPDATED:%u\n", new_period_s);
    }
  }
}

/*---------------------------------------------------------------------------*/
PROCESS_THREAD(iot_node_process, ev, data)
{
  static struct etimer periodic_timer;
  uip_ipaddr_t dest_ipaddr;

  PROCESS_BEGIN();

  /* Registrar conexión UDP */
  simple_udp_register(&udp_conn, UDP_CLIENT_PORT, NULL,
                      UDP_SERVER_PORT, udp_rx_callback);

  /* Espera inicial aleatoria para evitar colisiones */
  etimer_set(&periodic_timer, random_rand() % send_interval);

  while(1) {
    PROCESS_WAIT_EVENT_UNTIL(etimer_expired(&periodic_timer));

    /* Requisito a): comprobar cambio de padre en cada iteración */
    check_parent_change();

    if(NETSTACK_ROUTING.node_is_reachable() &&
       NETSTACK_ROUTING.get_root_ipaddr(&dest_ipaddr)) {

      /* Requisito b): enviar tráfico dummy periódico al root */
      seq_num++;
      LOG_INFO("MSG_SENT:%u\n", seq_num);
      simple_udp_sendto(&udp_conn, &seq_num, sizeof(seq_num), &dest_ipaddr);

    } else {
      LOG_INFO("Not reachable yet\n");
    }

    etimer_set(&periodic_timer, send_interval
      - CLOCK_SECOND + (random_rand() % (2 * CLOCK_SECOND)));
  }

  PROCESS_END();
}
/*---------------------------------------------------------------------------*/