from mpi4py import MPI
import numpy as np
import os
import zlib
import sys
from collections import defaultdict
WORKTAG = 1
DIETAG = 0
CHECKSUMFILE = "checksum.crc32"  # define the name of the checksum file



def master(comm):
    num_procs = comm.Get_size()
    status = MPI.Status()

    checksums = {}
    update = []
    subdir = []

    for root, dirs, files in os.walk("."):
        for name in dirs+["."]:
            checksum = os.path.join(root, name, CHECKSUMFILE)
            if os.path.exists(checksum):
                with open(checksum,encoding="utf-8") as f:  # load current checksums into memory
                    string = f.readline()[:-1]
                    while string:
                        items = string.split("\t")
			checksum = items[0]
			mtime = items[1]
			inode = items[2]
			filename = "\t".join(items[3:])
                        checksums[os.path.join(root,name,filename)] = (mtime, inode)
                        string = f.readline()[:-1]

    for root,dirs,files in os.walk("."):
        for name in files:
            filename = os.path.join(root,name)
            stat = os.stat(filename)
            now_mtime = str(stat.st_mtime)
            now_inode = str(stat.st_ino)
            if filename in checksums:
                mtime,inode = checksums[filename]
                if now_mtime != mtime or now_inode != inode:  # need recalculated
                    update.append(filename)  # todo sort it here.
            elif CHECKSUMFILE not in filename and ".py" not in filename:  # exclude .crc32 file
                update.append(filename)

    update.sort(key=lambda f: os.stat(f).st_size)  # sort the files for load balance.
    # Increasing order, because we use it as stack.

    results = defaultdict(list)

    #  handle the situation where the updated files is less than the number of processors.
    if len(update) < num_procs:
        for i in range(len(update)):
            comm.send(update[i], dest=i+1, tag=WORKTAG)
        for i in range(len(update)):
            result = comm.recv(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status)
            results[result[0]].append(result[1])
        for i in range(1,num_procs):
            comm.send(0,dest = i, tag=DIETAG)
    else:

        # send jobs to different slave
        for rank in range(1, num_procs):
            if update:
                filename = update.pop()
                comm.send(filename, dest=rank, tag=WORKTAG)
os.walk


        # The loop of receive and send.
        while True:
            if not update:
                break
            result = comm.recv(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status)  # receive results
            results[result[0]].append(result[1])
            comm.send(update.pop(), dest=status.Get_source(), tag=WORKTAG)

        # finish all works
        for rank in range(1, num_procs):
            result = comm.recv(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status)  # receive results
            results[result[0]].append(result[1])
        for rank in range(1, num_procs):
            comm.send(0, dest=rank, tag=DIETAG)


    for root in results:
        new_f = open(os.path.join(root,CHECKSUMFILE+"_tmp"),"w")
        if os.path.isfile(os.path.join(root,CHECKSUMFILE)):
            with open(os.path.join(root,CHECKSUMFILE)) as f:
                string = f.readline()
                while string:
                    new_f.write(string)
                    string = f.readline()
        for result in results[root]:
            new_f.write(result)
        new_f.close()
        os.rename(os.path.join(root, CHECKSUMFILE+"_tmp"), os.path.join(root,CHECKSUMFILE))





def slave(comm):  # The sub processors
    my_rank = comm.Get_rank()
    status = MPI.Status()

    while True:
        filename = comm.recv(source=0, tag=MPI.ANY_TAG, status=status)

        # if the received message if dir, change working dir.

        if status.Get_tag() == DIETAG:

            break


        # calculate the checksum, and send it back to the master.
        crc32 = get_crc32(filename)
        stat = os.stat(filename)

        root,file = os.path.split(filename)

        result = (root,"\t".join((str(crc32), str(stat.st_mtime), str(stat.st_ino),file)) + "\n")
        comm.send(result, dest=0, tag=0)


def get_crc32(filename):
    with open(filename, "rb") as file_to_check:
        # read contents of the file
        data = file_to_check.read(16777216)
        crc32 = 0
        while data:
            crc32 = zlib.crc32(data,crc32) 
            data = file_to_check.read(16777216)
        # pipe contents of the file through
        crc32_returned = "{:x}".format(crc32 & 0xffffffff).upper()
        return crc32_returned

def print_to_terminal():
    for root,dirs,files in os.walk ("."):
        for dir in dirs+["."]:
            checksumfile = os.path.join(root,dir,CHECKSUMFILE)
            if os.path.exists(checksumfile):
                with open(checksumfile) as f:
                    line = f.readline()
                    while line:
                        items = line.split("\t")
			checksum = items[0]
			mtime = items[1]
			inode = items[2]
			filename = "\t".join(items[3:])
                        filename = os.path.join(root,dir,filename)
                        print(" ".join((checksum,str(os.path.getsize(filename)),filename,filename)))
                        line = f.readline()
                                                                                                                                                                    
                   

if __name__ == "__main__":
    comm = MPI.COMM_WORLD
    my_rank = comm.Get_rank()
    if my_rank == 0:
        master(comm)
        print_to_terminal()
    else:
        slave(comm)
