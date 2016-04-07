# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json

from charms.reactive import RelationBase
from charms.reactive import hook
from charms.reactive import scopes

from charmhelpers.core import hookenv

from jujubigdata import utils


class NameNodePeers(RelationBase):
    scope = scopes.UNIT

    @hook('{peers:namenode-cluster}-relation-joined')
    def joined(self):
        conv = self.conversation()
        conv.set_state('{relation_name}.joined')
        conv.set_state('{relation_name}.initialized')

    @hook('{peers:namenode-cluster}-relation-departed')
    def departed(self):
        conv = self.conversation()
        conv.remove_state('{relation_name}.joined')

    def nodes(self):
        node_names = [hookenv.local_unit().replace('/', '-')]
        for conv in self.conversations():
            node_names.append(conv.scope.replace('/', '-'))
            node_names = sorted(node_names)
            stored_nodes = hookenv.leader_get('validated_namenodes')
            if not stored_nodes:
                stored_nodes = node_names[:2]
            if not hookenv.is_leader():
                return stored_nodes
            elif hookenv.is_leader():
                checked_nodes = []
                for node in node_names:
                    result = utils.ha_node_state(node)
                    if result and result == 'active' or result == 'standby':
                        checked_nodes.append(result)
                checked_nodes = sorted(checked_nodes)
                if len(checked_nodes) < 2:
                    hookenv.log("Only one node is responding, according to hadoop, HDFS HA is degraded...")
                    return stored_nodes
                if sorted(checked_nodes) == stored_nodes:
                    return stored_nodes
                else:
                    hookenv.leader_set(validated_namenodes=checked_nodes)
                    return checked_nodes

    def hosts_map(self):
        result = {}
        for conv in self.conversations():
            addr = conv.get_remote('private-address', '')
            ip = utils.resolve_private_address(addr)
            host_name = conv.scope.replace('/', '-')
            result.update({ip: host_name})
        return result

    def check_peer_port(self, port):
        return all(utils.check_peer_port(peer_ip, port)
                   for peer_ip in self.hosts_map().keys())

    def jns_init(self):
        for conv in self.conversations():
            conv.set_remote(data={
                'jns_ready': json.dumps(True),
            })

    def are_jns_init(self):
        return all(json.loads(conv.get_remote('jns_ready', 'false'))
                   for conv in self.conversations())

    def zookeeper_formatted(self):
        for conv in self.conversations():
            conv.set_remote(data={
                'zookeeper_formatted': json.dumps(True),
            })

    def is_zookeeper_formatted(self):
        return all(json.loads(conv.get_remote('zookeeper_formatted', 'false'))
                   for conv in self.conversations())
