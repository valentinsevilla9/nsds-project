/*
 * iot-node.c — Nodo IoT para el Digital Twin (Proyecto #2)
 * Versión corregida: usa solo APIs públicas de Contiki-NG
 */

#include "contiki.h"
#include "net/routing/routing.h"
#include "random.h"
#include "net/netstack.h"
#include "net/ipv6/simple-udp.h"
#include "net/ipv6/uip-ds6-route.h"
#include "sys/log.h"

#define LOG_MODULE "App"
#define LOG_LEVEL LOG_LEVEL_INFO

#define UDP_CLIENT_PORT  8765
#define UDP_SERVER_PORT  5678
#define DEFAULT_SEND_INTERVAL (4 * CLOCK_SECOND)

static struct simple_udp_connection udp_conn;
static clock_time_t send_interval = DEFAULT_SEND_INTERVAL;
static unsigned seq_num = 0;
static uip_ipaddr_t last_root;
static int has_last_root = 0;

PROCESS(iot_node_process, "IoT Node Digital Twin");
AUTOSTART_PROCESSES(&iot_node_process);

/*
 * Detecta cambio de ruta al root y lo reporta por serie.
 * Usamos get_root_ipaddr que es API pública.
 */
static void
check_parent_change(void)
{
  uip_ipaddr_t root_addr;
  if(!NETSTACK_ROUTING.get_root_ipaddr(&root_addr)) return;

  if(!has_last_root || !uip_ipaddr_cmp(&root_addr, &last_root)) {
    LOG_INFO("PARENT_CHANGE:");
    LOG_INFO_6ADDR(&root_addr);
    LOG_INFO_("\n");
    uip_ipaddr_copy(&last_root, &root_addr);
    has_last_root = 1;
  }
}

/*
 * Callback UDP: recibe comandos del root.
 * Formato: "PERIOD:<segundos>"
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

PROCESS_THREAD(iot_node_process, ev, data)
{
  static struct etimer periodic_timer;
  uip_ipaddr_t dest_ipaddr;

  PROCESS_BEGIN();

  simple_udp_register(&udp_conn, UDP_CLIENT_PORT, NULL,
                      UDP_SERVER_PORT, udp_rx_callback);

  etimer_set(&periodic_timer, random_rand() % send_interval);

  while(1) {
    PROCESS_WAIT_EVENT_UNTIL(etimer_expired(&periodic_timer));

    check_parent_change();

    if(NETSTACK_ROUTING.node_is_reachable() &&
       NETSTACK_ROUTING.get_root_ipaddr(&dest_ipaddr)) {

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