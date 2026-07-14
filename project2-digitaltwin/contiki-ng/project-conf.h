#ifndef PROJECT_CONF_H_
#define PROJECT_CONF_H_

/* Habilitar RPL classic (igual que en los ejemplos del curso) */
#define ROUTING_CONF_RPL_CLASSIC 1

/* El border router del curso anuncia Non-Storing Mode (MOP 1). Por defecto
 * nuestros nodos exigen coincidencia exacta con RPL_MOP_STORING_NO_MULTICAST
 * (2), y descartan cualquier DIO que no la tenga - hay que pedir el mismo
 * modo que usa el root para poder unirnos a su DODAG. */
#define RPL_CONF_MOP RPL_MOP_NON_STORING

/* Logging nivel INFO para ver los mensajes PARENT_CHANGE y MSG_SENT */
#define LOG_CONF_LEVEL_RPL LOG_LEVEL_WARN
#define LOG_CONF_LEVEL_TCPIP LOG_LEVEL_WARN
#define LOG_CONF_LEVEL_IPV6 LOG_LEVEL_WARN
#define LOG_CONF_LEVEL_6LOWPAN LOG_LEVEL_WARN
#define LOG_CONF_LEVEL_MAC LOG_LEVEL_WARN
#define LOG_CONF_LEVEL_FRAMER LOG_LEVEL_WARN

#endif /* PROJECT_CONF_H_ */