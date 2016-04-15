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

    @hook('{peers:namenode-cluster}-relation-changed')
    def changed(self):
        conv = self.conversation()
        if conv.get_remote('standby-ready'):
            conv.set_state('{relation_name}.standby.ready')

    @hook('{peers:namenode-cluster}-relation-departed')
    def departed(self):
        conv = self.conversation()
        conv.remove_state('{relation_name}.joined')

    def nodes(self):
        return sorted(list(conv.units)[0].replace('/', '-')
                      for conv in self.conversations())

    def standby_ready(self):
        for conv in self.conversations():
            conv.set_remote('standby-ready', 'true')

    def hosts_map(self):
        local_host_name = hookenv.local_unit().replace('/', '-')
        local_ip = utils.resolve_private_address(hookenv.unit_private_ip())
        result = {local_ip: local_host_name}
        for conv in self.conversations():
            addr = conv.get_remote('private-address', '')
            ip = utils.resolve_private_address(addr)
            host_name = list(conv.units)[0].replace('/', '-')
            result.update({ip: host_name})
        return result
