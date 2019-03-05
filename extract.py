import json
import argparse

import re

DS_PATTERN = re.compile(r"^\[PutDSCommittee      \] \[\s*(\d+)\] (\w+) <(.+)>$")
SHARD_PATTERN = re.compile(r"^\[LoadShardingStructur\] \[\s*(\d+)\] (\w+) <(.+)>$")
VCDS_PATTERN = re.compile(r"^\[ProcessVCDSBlocksMes\] \[Epoch (\d+)] lastBlockHash \d+, new DS leader Id \d+$")

class MembershipParser:

    def __init__(self, filename, output):
        self.fd = open(filename)
        self.output_filename = output
        self.epochs = {}
        self.current_epoch = None


    def process_vcdsblock(self, entry):
        m = VCDS_PATTERN.match(entry)
        if m is None:
            return

        epoch_number = m.group(1)
        self.current_epoch = int(epoch_number)

        if self.current_epoch in self.epochs:
            return

        epoch_template = {"ds": {}, "shard": set()}
        for i in range(600):
            epoch_template["ds"][i] = None
        self.epochs[self.current_epoch] = epoch_template
        print "Processing epoch %d..." % self.current_epoch


    def process_dsentry(self, entry):
        if self.current_epoch is None:
            return

        m = DS_PATTERN.match(entry)
        if m is None:
            return

        index = int(m.group(1))
        pubkey = m.group(2)
        network_info = m.group(3)

        self.epochs[self.current_epoch]["ds"][index] = (pubkey, network_info)


    def process_shardentry(self, entry):
        if self.current_epoch is None:
            return

        m = SHARD_PATTERN.match(entry)
        if m is None:
            return

        # index = m.group(1) # We do not know which shard this shard member belongs to.
        pubkey = m.group(2)
        network_info = m.group(3)

        self.epochs[self.current_epoch]["shard"].add((pubkey, network_info))


    def parse_entry(self, entry):
        if "ProcessVCDSBlocksMes" in entry:
            self.process_vcdsblock(entry)
        elif "PutDSCommittee" in entry:
            self.process_dsentry(entry)
        elif "LoadShardingStructur" in entry:
            self.process_shardentry(entry)


    def write_entries(self):
        output_fd = open(self.output_filename, "w")
        for epoch in self.epochs:
            current_entry = self.epochs[epoch]
            current_entry["shard"] = list(current_entry["shard"])
        json.dump(self.epochs, output_fd)


    def run(self):
        for index, line in enumerate(self.fd):
            entry = json.loads(line)["line"]
            self.parse_entry(entry)

        self.write_entries()

def main():
    parser = argparse.ArgumentParser(description='Process some shard and DS membership data.')
    parser.add_argument('filename')
    parser.add_argument('output')

    args = parser.parse_args()
    mp = MembershipParser(args.filename, args.output)
    mp.run()

if __name__ == '__main__':
    main()
