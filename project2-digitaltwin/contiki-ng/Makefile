CONTIKI_PROJECT = iot-node
all: $(CONTIKI_PROJECT)

# Ruta al repositorio Contiki-NG clonado en la VM
CONTIKI = $(HOME)/nsds-contiki-ng

# project-conf.h ya pide RPL Classic con ROUTING_CONF_RPL_CLASSIC, pero esa
# macro sola no basta para que se compilen/enlacen los .c de RPL Classic
# (p.ej. rpl_parent_get_ipaddr, en rpl-dag.c) - hace falta tambien esto:
MAKE_ROUTING = MAKE_ROUTING_RPL_CLASSIC

include $(CONTIKI)/Makefile.include