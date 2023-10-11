#include "parser.h"
#include <yaml.h>
#include <errno.h>
#include <assert.h>
#include <stdint.h>
#include <unistd.h>
#include <net/if.h>
#include <arpa/inet.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <tbc.h>
#include "log.h"

int node_id = -1;
int majority = -1;
int nr_nodes = -1;
int eval_intv = -1;
int client_port = -1;
int tracker_port = -1;
int notifier_port = -1;
int replayer_port = -1;
int listener_port = -1;
int generator_port = -1;
int collector_port = -1;
int heartbeat_port = -1;
int evaluator_port = -1;

bool quiet = true;
char log_name[1024];
char iface[IFNAME_SIZE];
bool alive_node[NODE_MAX];
bitmap_t available_nodes = 0;
bitmap_t node_mask[NODE_MAX];
char nodes[NODE_MAX][IPADDR_SIZE];

struct in_addr parser_get_ifaddr()
{
    int fd;
    struct ifreq ifr;

    fd = socket(AF_INET, SOCK_DGRAM, 0);
    ifr.ifr_addr.sa_family = AF_INET;
    strncpy(ifr.ifr_name, iface, IFNAMSIZ - 1);
    ioctl(fd, SIOCGIFADDR, &ifr);
    close(fd);
    return ((struct sockaddr_in *)&ifr.ifr_addr)->sin_addr;
}


int parser_get_servers(yaml_node_t *start, yaml_node_t *node)
{
    int i;
    int cnt = 0;
    yaml_node_item_t *item;

    if (node->type != YAML_SEQUENCE_NODE) {
        log_err("failed to parse servers");
        return -EINVAL;
    }

    for (item = node->data.sequence.items.start; item != node->data.sequence.items.top; item++) {
        if (cnt > NODE_MAX) {
            log_err("failed to parser servers (too many nodes)");
            return -EINVAL;
        }
        strncpy(nodes[cnt], (char *)start[*item - 1].data.scalar.value, IPADDR_SIZE - 1);
        cnt++;
    }

    if (!cnt) {
        log_err("failed to parse servers");
        return -EINVAL;
    }

    nr_nodes = cnt;
    majority = (cnt + 1) / 2;

    for (i = 0; i < nr_nodes; i++)
        node_mask[i] = 1 << i;

    available_nodes = (bitmap_t)((1 << nr_nodes) - 1);
    for (i = 0; i < nr_nodes; i++)
        alive_node[i] = true;

    return 0;
}


int parser_get_iface(yaml_node_t *start, yaml_node_t *node)
{
    char *str = (char *)node->data.scalar.value;

    strcpy(iface, str);
    return 0;
}


int parser_get_ports(yaml_node_t *start, yaml_node_t *node)
{
    yaml_node_pair_t *p;

    for (p = node->data.mapping.pairs.start; p < node->data.mapping.pairs.top; p++) {
        yaml_node_t *key = &start[p->key - 1];
        yaml_node_t *val = &start[p->value - 1];
        char *key_str = (char *)key->data.scalar.value;
        char *val_str = (char *)val->data.scalar.value;

        if (!strcmp(key_str, "client")) {
            client_port = strtol(val_str, NULL, 10);
        } else if (!strcmp(key_str, "generator")) {
            generator_port = strtol(val_str, NULL, 10);
        } else if (!strcmp(key_str, "notifier")) {
            notifier_port = strtol(val_str, NULL, 10);
        } else if (!strcmp(key_str, "replayer")) {
            replayer_port = strtol(val_str, NULL, 10);
        } else if (!strcmp(key_str, "listener")) {
            listener_port = strtol(val_str, NULL, 10);
        } else if (!strcmp(key_str, "collector")) {
            collector_port = strtol(val_str, NULL, 10);
        } else if (!strcmp(key_str, "heartbeat")) {
            heartbeat_port = strtol(val_str, NULL, 10);
        } else if (!strcmp(key_str, "evaluator")) {
            evaluator_port = strtol(val_str, NULL, 10);
        } else if (!strcmp(key_str, "tracker")) {
            tracker_port = strtol(val_str, NULL, 10);
        }
    }
    if ((-1 == client_port)|| (-1 == generator_port)
        || (-1 == notifier_port) || (-1 == replayer_port)
        || (-1 == collector_port) || (-1 == heartbeat_port) || (-1 == tracker_port) ) {
        log_debug("ivalid port settings");
        return -EINVAL;
    }
    return 0;
}

int parse()
{
    FILE *fp;
    int ret = 0;
    yaml_node_t *start;
    yaml_document_t doc;
    yaml_node_pair_t *p;
    yaml_parser_t parser;

    fp = fopen(PATH_CONF, "rb");
    if (!fp) {
        log_err("cannot find %s", PATH_CONF);
        return -1;
    }
    yaml_parser_initialize(&parser);
    yaml_parser_set_input_file(&parser, fp);
    if (!yaml_parser_load(&parser, &doc)) {
        log_err("failed to parse %s", PATH_CONF);
        ret = -1;
        goto out;
    }
    start = doc.nodes.start;
    if (start->type != YAML_MAPPING_NODE) {
        log_err("faind to load %s", PATH_CONF);
        ret = -1;
        goto out;
    }
    for (p = start->data.mapping.pairs.start; p < start->data.mapping.pairs.top; p++) {
        int ret = -1;
        yaml_node_t *key = &start[p->key - 1];
        yaml_node_t *val = &start[p->value - 1];
        char *key_str = (char *)key->data.scalar.value;

        if (!strcmp(key_str, "iface"))
            ret = parser_get_iface(start, val);
        else if (!strcmp(key_str, "ports"))
            ret = parser_get_ports(start, val);
        else if (!strcmp(key_str, "servers"))
            ret = parser_get_servers(start, val);
        if (ret)
            break;
    }
    if (!ret) {
        for (int i = 0; i < nr_nodes; i++) {
            if (!strcmp(nodes[i], inet_ntoa(parser_get_ifaddr()))) {
                log_debug("srv%d: %s <", i, nodes[i]);
                node_id = i;
            } else
                log_debug("srv%d: %s", i, nodes[i]);
        }
    }
out:
    yaml_parser_delete(&parser);
    fclose(fp);
    return ret;
}
