#!/usr/bin/env python
import sys
sys.path.append('/opt/lib')
from subprocess import Popen, PIPE
try:
    from execute import execute
except ImportError:
    # Fallback if execute module not found
    def execute(args):
        p = Popen(args, stdout=PIPE, stderr=PIPE, close_fds=True)
        output, err = p.communicate()
        rc = p.returncode
        p.stdout.close()
        p.stderr.close()
        out = output.decode('utf-8')
        return out, err, rc

import re
# Commented out - modules not available
# from pv import pv
# from compression import compressor, uncompressor, uncompressor_file
from datetime import datetime
import os
import shutil
from collections import OrderedDict


# def execute(args):
#    p = Popen(args, stdout=PIPE, stderr=PIPE)
#    output, err = p.communicate()
#    rc = p.returncode
#    return output,err,rc


class zfs():
    def is_zfs(self, dataset):
        out, err, rc = execute(['zfs', 'list', '-H', '-p', dataset])
        list = []
        for i in out.split('\t'):
            list.append(i)
        if list and rc == 0 and list[0] == dataset:
            return True
        else:
            return False

    def type(self, dataset):
        props = self.get_all(dataset)
        if props:
            if not 'origin' in props.keys():
                return [props['type'], 'original']
            else:
                return [props['type'], 'clone']

    def snapshot(self, dataset, snap, recurse=False):
        if not recurse:
            out, err, rc = execute(['zfs', 'snapshot', dataset + '@' + snap])
        else:
            out, err, rc = execute(['zfs', 'snapshot', '-r', dataset + '@' + snap])

        return rc


    def snapshot_auto(self,dataset,tag,tag1=None, recurse=False):
        now = datetime.utcnow()
        name = tag

        if tag1:
            name += '_'
            name += str(tag1)
        name += '_'

        time_s = now.strftime('%Y-%m-%d-%H-%M')

        name += time_s

        rc = self.snapshot(dataset,name,recurse)

        return rc, name


    def autoremove(self,dataset,keep=2,tag=None,recurse=False, tags={}):
        snaps = self.get_snapshots(dataset)

        if tag:
            new_snaps = []
            for snap in snaps:
                if tag in snap:
                    new_snaps.append(snap)
            snaps = new_snaps

            if snaps and len(snaps) > keep:
                del snaps[len(snaps) - keep:]

        elif tags:
            new_snaps = []
            for tag in tags.keys():
                keep = tags[tag]
                tag_snaps = []
                for snap in snaps:
                    if tag in snap:
                        tag_snaps.append(snap)

                if tag_snaps and len(tag_snaps) > keep:
                    del tag_snaps[len(tag_snaps) - keep:]
                    new_snaps.extend(tag_snaps)

            snaps = new_snaps

        else:
            if snaps and len(snaps) > keep:
                del snaps[len(snaps) - keep:]

        for snap in snaps:
            try:
                self.destroy(dataset+'@'+snap,recurse=recurse)
            except:
                pass




    def rollback(self, dataset, snap):
        out, err, rc = execute(['zfs', 'rollback', '-r', dataset + '@' + snap])
        return rc

    def restore_files(self,dataset,snap,file_list=[]):
        ret = 1
        if self.is_zfs(dataset):
            zfs_dir = os.path.join('/'+dataset,'.zfs')
            if not os.path.isdir(zfs_dir):
                self.set(dataset,'snapdir','visible')

            restore_path = os.path.join(zfs_dir,'snapshot',snap)

            if os.path.exists(restore_path):
                ret=0
                for f in file_list:
                    f_old = os.path.join(restore_path,f.split('/'+dataset)[1][1:])

                    shutil.copy(f_old,f)
        return ret




    def get_snapshots(self, dataset):
        result = []
        out, err, rc = execute(['zfs', 'list', '-t', 'snapshot', '-H', '-o', 'name'])
        if rc == 0:
            for i in out.splitlines():
                if dataset == i.split('@')[0]:
                    result.append(i.split('@')[1])

        return result

    def set(self, dataset, property, value):
        out, err, rc = execute(['zfs', 'set', property + '=' + value, dataset])
        return rc

    def get(self, dataset=None, property='all'):
        if dataset:
            out, err, rc = execute(['zfs', 'get', '-H', '-p', property, dataset])
            if out.split('\t')[2] != '-':
                value = out.split('\t')[2]
            else:
                value = None

            return value
        else:
            list = {}
            out, err, rc = execute(['zfs', 'get', '-H', property])
            for i in out.splitlines():

                if i.split('\t')[2] != '-':
                    list[i.split('\t')[0]] = i.split('\t')[2]
                else:
                    list[i.split('\t')[0]] = None

            return list

    def get_all(self, dataset):
        out, err, rc = execute(['zfs', 'get', '-H', 'all', dataset])
        list = {}
        for i in out.splitlines():
            list[i.split('\t')[1]] = i.split('\t')[2]

        return list
    
    def get_space(self, dataset):
        """Get space usage information for a dataset"""
        # Get available and used space
        out_avail, err, rc = execute(['zfs', 'get', '-H', '-p', 'available', dataset])
        out_used, err, rc2 = execute(['zfs', 'get', '-H', '-p', 'used', dataset])
        
        result = {'avail': 0, 'used': 0}
        
        if rc == 0 and out_avail:
            # Parse available space
            parts = out_avail.strip().split('\t')
            if len(parts) >= 3:
                result['avail'] = int(parts[2])
                
        if rc2 == 0 and out_used:
            # Parse used space
            parts = out_used.strip().split('\t')
            if len(parts) >= 3:
                result['used'] = int(parts[2])
                
        return result

    def list(self, dataset=None):
        list = []
        if dataset == None:
            out, err, rc = execute(['zfs', 'list', '-H'])
        else:
            out, err, rc = execute(['zfs', 'list', '-H', '-r', dataset])
        for i in out.splitlines():
            list.append(i.split('\t')[0])
        return list

    def list_volumes(self):
        list = []
        out,err,rc = execute(['zfs','list','-t','volume','-H'])
        for i in out.splitlines():
            list.append(i.split('\t')[0])
        return list

    def clone(self, dataset, clone, property=None):
        if '@' in dataset:
            com = ['zfs', 'clone']
            if property:
                for i in property.keys():
                    com.append('-o')
                    com.append(i + '=' + property[i])
            com.append(dataset)
            com.append(clone)
            out, err, rc = execute(com)
            return rc
        else:
            snap = clone.split('/')[-1]
            snap_out, snap_err, snap_rc = execute(['zfs', 'snapshot', dataset + '@' + snap])
            com = ['zfs', 'clone']
            if property:
                for i in property.keys():
                    com.append('-o')
                    com.append(i + '=' + property[i])
            com.append(dataset + '@' + snap)
            com.append(clone)
            clone_out, clone_err, clone_rc = execute(com)
            if snap_rc == 0 and clone_rc == 0:
                return 0
            else:
                return 1

    def create(self, dataset, property=None):
        # IMPROVEMENT 4: Add debug logging for ZFS create operations
        import logging
        logger = logging.getLogger('zfs_manager')
        
        logger.debug(f"ZFS create called for dataset: {dataset}")
        
        com = ['zfs', 'create']
        if property:
            logger.debug(f"  Properties: {property}")
            for i in property.keys():
                com.append('-o')
                com.append(i + '=' + property[i])
        com.append(dataset)
        
        logger.debug(f"  Command: {' '.join(com)}")
        
        out, err, rc = execute(com)
        
        if rc != 0:
            logger.error(f"ZFS create failed for {dataset}")
            logger.error(f"  Return code: {rc}")
            logger.error(f"  Error output: {err}")
            logger.debug(f"  Standard output: {out}")
            
            # Store error for dataset busy detection
            if hasattr(self, '__dict__'):
                self.last_error = err if err else f"return code {rc}"
        else:
            logger.info(f"ZFS create successful for {dataset}")
            if hasattr(self, '__dict__'):
                self.last_error = None
                
        return rc

    def rename(self, dataset, dataset1):
        com = ['zfs', 'rename', dataset, dataset1]
        out, err, rc = execute(com)
        return rc

    def zvol_create(self, dataset, size=0, compression='lz4',bytes=0,volblocksize='8K'):
        if size:
            out, err, rc = execute(
                ['zfs', 'create', '-o', 'compression=' + compression, '-b', volblocksize, '-s', '-V', str(size) + 'G', dataset])
        elif bytes:
            out, err, rc = execute(
                ['zfs', 'create', '-o', 'compression=' + compression, '-s', '-V', str(bytes), '-b', volblocksize,
                 dataset])
        else:
            rc = -1
        return rc


    def share(self, dataset):
        out, err, rc = execute(['zfs', 'share', 'dataset'])
        return rc

    def unshare(self, dataset):
        out, err, rc = execute(['zfs', 'unshare', 'dataset'])
        return rc

    def mount(self, dataset):
        """Mount a ZFS dataset"""
        import logging
        logger = logging.getLogger('zfs_manager')
        
        logger.debug(f"ZFS mount called for dataset: {dataset}")
        
        out, err, rc = execute(['zfs', 'mount', dataset])
        
        if rc != 0:
            logger.error(f"ZFS mount failed for {dataset}")
            logger.error(f"  Return code: {rc}")
            logger.error(f"  Error output: {err}")
            logger.debug(f"  Standard output: {out}")
        else:
            logger.info(f"Successfully mounted ZFS dataset: {dataset}")
            
        return rc

    def destroy(self, dataset, recurse=False):
        # IMPROVEMENT 4: Add debug logging for ZFS destroy operations
        import logging
        logger = logging.getLogger('zfs_manager')
        
        logger.debug(f"ZFS destroy called for dataset: {dataset}, recurse={recurse}")
        
        if recurse == False:
            out, err, rc = execute(['zfs', 'destroy', dataset])
        else:
            out, err, rc = execute(['zfs', 'destroy', '-R', dataset])

        if rc != 0:
            # Log the error details
            logger.error(f"ZFS destroy failed for {dataset}")
            logger.error(f"  Return code: {rc}")
            logger.error(f"  Error output: {err}")
            logger.debug(f"  Standard output: {out}")
            
            # Store error for dataset busy detection
            if hasattr(self, '__dict__'):
                self.last_error = err if err else f"return code {rc}"
            
            print(out, err, rc)
        else:
            logger.info(f"ZFS destroy successful for {dataset}")
            if hasattr(self, '__dict__'):
                self.last_error = None
                
        return rc

    def promote(self, dataset, recurse=False):
        if recurse == False:
            out, err, rc = execute(['zfs', 'promote', dataset])
        else:
            zlist = self.list(dataset)
            for fs in zlist:
                out, err, rc = execute(['zfs', 'promote', fs])
        return rc

    def diff(self, snap1, snap2=None):
        if snap2 == None:
            out, err, rc = execute(['zfs', 'diff', '-HF', snap1])
        else:
            out, err, rc = execute(['zfs', 'diff', '-HF', snap1, snap2])
        new = []
        mod = []
        err = []
        ren = []
        if rc == 0:
            for line in out.splitlines():
                args = line.split('\t')
                if args[0] == '+':
                    new.append((args[2], args[1]))

                elif args[0] == '-':
                    err.append((args[2], args[1]))

                elif args[0] == 'M':

                    mod.append((args[2], args[1]))

                elif args[0] == 'R':
                    ren.append((args[2], args[3], args[1]))

                else:
                    pass

        return new, mod, err, ren

    def get_space(self, dataset):
        space = {}
        out, err, rc = execute(['zfs', 'list', '-H', '-p', '-o', 'space', dataset])
        elem = out.split('\t')

        space['name'] = elem[0]
        space['avail'] = int(elem[1])
        space['used'] = int(elem[2])
        space['usedsnap'] = int(elem[3])
        space['useddss'] = int(elem[4])
        space['usedrefreserv'] = int(elem[5])
        space['usedchild'] = int(elem[6].strip('\n'))

        return space

    def conv_space(self, space):
        if space[-1] == 'K':
            return int(float(space[:-1].replace(',', '.')) * 1024)

        if space[-1] == 'M':
            return int(float(space[:-1].replace(',', '.')) * 1024 * 1024)

        if space[-1] == 'G':
            return int(float(space[:-1].replace(',', '.')) * 1024 * 1024 * 1024)

        if space[-1] == 'T':
            return int(float(space[:-1].replace(',', '.')) * 1024 * 1024 * 1024 * 1024)

    def hold(self, dataset, snapshot, tag, recurse=False):
        if self.is_zfs(dataset):
            if recurse:
                out, err, rc = execute(['zfs', 'hold', '-r', tag, dataset + '@' + snapshot])
            else:
                out, err, rc = execute(['zfs', 'hold', tag, dataset + '@' + snapshot])

            return rc
        else:
            return -1

    def release(self, dataset, snapshot, tag, recurse=False):
        if self.is_zfs(dataset):
            if recurse:
                out, err, rc = execute(['zfs', 'release', '-r', tag, dataset + '@' + snapshot])
            else:
                out, err, rc = execute(['zfs', 'release', tag, dataset + '@' + snapshot])

            return rc
        else:
            return -1

    def holds(self, dataset, snapshot, recurse=False):
        holds = []
        if self.is_zfs(dataset):
            if recurse:
                out, err, rc = execute(['zfs', 'holds', '-H', '-r', dataset + '@' + snapshot])
            else:
                out, err, rc = execute(['zfs', 'holds', '-H', dataset + '@' + snapshot])

            if rc == 0 and out:

                for line in sorted(out.splitlines(),reverse=True):
                    holds.append(line.split('\t')[1])

        return holds

    def get_holds(self, dataset, recurse=False):
        snaps = self.get_snapshots(dataset)
        holds = OrderedDict()
        for s in snaps:
            hold = self.holds(dataset, s, recurse)
            if hold:
                holds[s] = hold

        return holds

    def send(self, dataset, snap, snap1=None, recurse=True, resume_token=None, 
             raw=None, compressed=None, resumable=True):
        """
        Send a ZFS snapshot with automatic detection of optimal flags.
        
        Args:
            dataset: The dataset to send
            snap: The snapshot to send
            snap1: Optional second snapshot for incremental send
            recurse: Whether to send recursively (default: True)
            resume_token: Resume token for resumable sends
            raw: Send raw encrypted stream. None=autodetect, True/False=force
            compressed: Send compressed blocks. None=autodetect, True/False=force
            resumable: Create resumable send stream -s flag (default: True)
        """
        cmd = []
        if resume_token:
            cmd.append('zfs')
            cmd.append('send')
            cmd.append('-t')
            cmd.append(resume_token)
        else:
            if self.is_zfs(dataset):
                snaps = self.get_snapshots(dataset)
                if snap in snaps:
                    cmd.append('zfs')
                    cmd.append('send')
                    
                    # Autodetect or use explicit raw flag
                    use_raw = raw
                    if use_raw is None:
                        # Autodetect: check if dataset is encrypted
                        try:
                            encryption = self.get(dataset, 'encryption')
                            use_raw = encryption is not None and encryption != 'off'
                        except:
                            use_raw = False
                    
                    if use_raw:
                        cmd.append('-w')
                    
                    # Autodetect or use explicit compressed flag
                    use_compressed = compressed
                    if use_compressed is None:
                        # Autodetect: check if dataset uses compression
                        try:
                            compression = self.get(dataset, 'compression')
                            use_compressed = compression is not None and compression != 'off'
                        except:
                            use_compressed = False
                    
                    if use_compressed:
                        cmd.append('-c')
                    
                    # Note: resumable flag (-s) is for receive operations only
                    # For resumable sends, use receive -s and then send -t <token>
                    
                    if recurse:
                        cmd.append('-R')

                    if not snap1:
                        cmd.append(dataset + '@' + snap)
                    else:
                        if snap1 in snaps:
                            cmd.append('-I')
                            cmd.append(dataset + '@' + snap)
                            cmd.append(dataset + '@' + snap1)
                        else:
                            cmd = []
        if cmd:
            p = Popen(cmd, stdout=PIPE)
            return p
        else:
            return None

    def get_send_size(self, dataset, snap, snap1=None, recurse=True, resume_token=None,
                      raw=None, compressed=None, resumable=False):
        """
        Get estimated size of a ZFS send operation with automatic flag detection.
        
        Args:
            dataset: The dataset to estimate
            snap: The snapshot to send
            snap1: Optional second snapshot for incremental send
            recurse: Whether to send recursively (default: True)
            resume_token: Resume token for resumable sends
            raw: Send raw encrypted stream. None=autodetect, True/False=force
            compressed: Send compressed blocks. None=autodetect, True/False=force
            resumable: Create resumable send stream -s flag (default: True)
        """
        cmd = []
        if resume_token:
            cmd.append('zfs')
            cmd.append('send')
            cmd.append('-t')
            cmd.append(resume_token)
            cmd.append('-nv')
        else:
            if self.is_zfs(dataset):
                snaps = self.get_snapshots(dataset)
                if snap in snaps:
                    cmd.append('zfs')
                    cmd.append('send')
                    
                    # Autodetect or use explicit raw flag
                    use_raw = raw
                    if use_raw is None:
                        # Autodetect: check if dataset is encrypted
                        try:
                            encryption = self.get(dataset, 'encryption')
                            use_raw = encryption is not None and encryption != 'off'
                        except:
                            use_raw = False
                    
                    if use_raw:
                        cmd.append('-w')
                    
                    # Autodetect or use explicit compressed flag
                    use_compressed = compressed
                    if use_compressed is None:
                        # Autodetect: check if dataset uses compression
                        try:
                            compression = self.get(dataset, 'compression')
                            use_compressed = compression is not None and compression != 'off'
                        except:
                            use_compressed = False
                    
                    if use_compressed:
                        cmd.append('-c')
                    
                    # Note: resumable flag (-s) is for receive operations only
                    # For resumable sends, use receive -s and then send -t <token>
                    
                    if recurse:
                        cmd.append('-R')
                    
                    cmd.append('-nv')
                    
                    if not snap1:
                        cmd.append(dataset + '@' + snap)
                    else:
                        if snap1 in snaps:
                            cmd.append('-I')
                            cmd.append(dataset + '@' + snap)
                            cmd.append(dataset + '@' + snap1)
                        else:
                            cmd = []
        if cmd:
            out, err, rc = execute(cmd)
            if rc == 0:
                return self.conv_space(out.splitlines()[-1].split()[-1])
            else:
                return None
        else:
            return None


    def recv(self, dataset, force=True):
        cmd = []
        cmd.append('zfs')
        cmd.append('recv')
        cmd.append(dataset)
        if force:
            cmd.append('-F')
        if cmd:
            p = Popen(cmd, stdin=PIPE)
            return p
        else:
            return None

    def recv_pipe(self,fd,dataset,force=True):
        cmd = []
        cmd.append('zfs')
        cmd.append('recv')
        cmd.append(dataset)
        if force:
            cmd.append('-F')
        if cmd:
            p = Popen(cmd, stdin=fd)
            return p
        else:
            return None

    def engociate_inc_send(self, dataset, recv_snapshots=[]):
        init_snap = None
        last_snap = None
        snapshots = []
        if self.is_zfs(dataset):
            snapshots = self.get_snapshots(dataset)
            if snapshots:
                for snap in recv_snapshots:
                    if snap in snapshots:
                        init_snap = snap

        if init_snap:
            last_snap = snapshots[-1]

            # if init_snap == last_snap:
            #     init_snap = None
            #     last_snap = None

        return init_snap, last_snap

    # Commented out - requires pv and compression modules
    # def adaptive_send(self, dataset=None, snap=None, snap1=None, recurse=True, compression=None, verbose=False, limit=0,
    #                   time=0,out_fd=None,resume_token=None):
    #
    #     size = self.get_send_size(dataset,snap,snap1,recurse,resume_token=resume_token)
    #     send = self.send(dataset, snap, snap1, recurse,resume_token=resume_token)
    #
    #     if send:
    #
    #
    #         if compression:
    #             #
    #             # piper = pv(send.stdout, verbose=verbose, limit=limit, size=size, time=time)
    #             #
    #             # if out_fd:
    #             #     compr = compressor(piper.stdout, compression,out_fd=out_fd)
    #             # else:
    #             #     compr = compressor(piper.stdout, compression)
    #
    #             comp = compressor(send.stdout,compression)
    #
    #             if out_fd:
    #                 piper = pv(comp.stdout, verbose=verbose, limit=limit, size=size, time=time,out_fd=out_fd)
    #             else:
    #                 piper = pv(comp.stdout, verbose=verbose, limit=limit, size=size, time=time)
    #
    #             return piper
    #         else:
    #             if out_fd:
    #                 piper = pv(send.stdout, verbose=verbose, limit=limit, size=size, time=time,out_fd=out_fd)
    #             else:
    #                 piper = pv(send.stdout, verbose=verbose, limit=limit, size=size, time=time)
    #
    #             return piper


class zpool():
    def list(self):
        list = []
        out, err, rc = execute(['zpool', 'list', '-H', '-o', 'name'])
        for i in out.splitlines():
            list.append(i)

        return list

    def get(self, zpool, property='all'):
        if zpool:
            out, err, rc = execute(['zpool', 'get', '-H', '-p', property, zpool])
            value = out.split('\t')[2]
            return value
        else:
            list = {}
            out, err, rc = execute(['zpool', 'get', '-H', '-p', property])
            for i in out.splitlines():
                if i.split('\t')[2] != '-':
                    list[i.split('\t')[0]] = i.split('\t')[2]

            return list

    def get_all(self, zpool):
        out, err, rc = execute(['zpool', 'get', '-H', '-p', 'all', zpool])
        list = {}
        for i in out.splitlines():
            list[i.split('\t')[1]] = i.split('\t')[2]

        return list

    def set(self, zpool, property, value):

        out, err, rc = execute(['zpool', 'set', property + '=' + value, zpool])

        return rc

    def start_scrub(self, zpool):
        out, err, rc = execute(['zpool', 'scrub', zpool])
        return rc

    def stop_scrub(self, zpool):
        out, err, rc = execute(['zpool', 'scrub', '-s', zpool])
        return rc

    def clear(self, zpool, drive=None):
        if drive:
            out, err, rc = execute(['zpool', 'clear', zpool, drive])
        else:
            out, err, rc = execute(['zpool', 'clear', zpool])

        return rc

    def online(self, zpool, drive):
        if drive:
            out, err, rc = execute(['zpool', 'online', zpool, drive])

        return rc

    def zimport(self, zpool=None, force=False, mount=False, persist='id'):

        com = ['zpool', 'import']
        if zpool:
            com.append(zpool)
        else:
            com.append('-a')

        if force:
            com.append('-f')

        if not mount:
            com.append('-N')

        if persist == 'path':
            com.append('-d')
            com.append('/dev/disk/by-path')

        elif persist == 'id':
            com.append('-d')
            com.append('/dev/disk/by-id')

        else:
            com.append('-d')
            com.append(persist)

        out, err, rc = execute(com)
        return rc


class zdb:
    def __init__(self):
        self.zpools = {}
        out, err, rc = execute(['zdb'])
        self.lines = out.splitlines()
        for line in self.lines:
            m = re.match('(\S+):', line)
            if m:
                name = m.group(1)
                self.zpools[name] = []
            else:

                self.zpools[name].append(line)



    def get_zpools(self):

        return self.zpools.keys()





    def get_drives(self, zpool):
        if zpool in self.zpools.keys():
            disks = []
            for line in self.zpools[zpool]:
                m = re.match("\s*path: '(\S+)'", line)
                if m:
                    disk = m.group(1)

                w = re.match("\s*whole_disk: (\d+)", line)
                if w:
                    if int(w.group(1)):
                        if 'part' in disk:
                            disk = disk[:-6]
                        else:
                            disk = disk[:-1]
                    else:

                        pass

                    disks.append(disk)
        else:
            disks = []

        return disks

    def get_guid(self, zpool, drive):
        if zpool in self.zpools.keys():
            for line in self.zpools[zpool]:
                m = re.match('\s*guid: (\d+)', line)
                if m:
                    guid = m.group(1)

                m = re.match("\s*path: '%s'" % drive, line)
                if m:
                    return guid
        else:
            return None

    def get_zpool_state(self, zpool):
        status, err, rc = execute(['zpool', 'status', zpool, '-v'])
        if rc == 0:
            for line in status.splitlines():
                m = re.match('\s*%s\s*(\S+)' % zpool, line)
                if m:
                    state = m.group(1)
                    return state

    def get_disk_state(self, zpool, drive):
        status, err, rc = execute(['zpool', 'status', zpool, '-v'])
        if rc == 0:
            for line in status.splitlines():
                m = re.match('\s*%s\s*(\S+)' % drive, line)
                if m:
                    state = m.group(1)
                    break
                else:
                    state = 'UNAVAIL'
            return state

    def get_guid_state(self,zpool,guid):
        status, err, rc = execute(['zpool', 'status', zpool, '-vg'])
        if rc == 0:
            for line in status.splitlines():
                m = re.match('\s*%s\s*(\S+)' % guid, line)
                if m:
                    state = m.group(1)
                    if 'repairing' in line:
                        state = 'repairing'
                    break
                else:
                    state = 'UNAVAIL'
            return state

    def is_missing(self, zpool):
        missing = False
        status, err, rc = execute(['zpool', 'status', zpool, '-v'])
        if rc == 0:
            for line in status.splitlines():
                if 'UNAVAIL' in line:
                    missing = True
            return missing

    def get_missing(self, zpool):
        guid = None
        status, err, rc = execute(['zpool', 'status', zpool, '-v'])
        if rc == 0:
            for line in status.splitlines():
                m = re.match('\s*(\d*)\s* UNAVAIL', line)
                if m:
                    guid = m.group(1)

        return guid

    def heal(self, zpool, old_disk, new_disk):

        pass

    def replace(self, zpool, old_disk, new_disk):
        out, err, rc = execute(['zpool', 'replace', zpool, old_disk, new_disk, '-f'])
        if rc != 0:
            if 'is part of active pool' in err:
                rc == 2
        return rc

    def is_resilvering(self, zpool):
        status, err, rc = execute(['zpool', 'status', zpool, '-v'])
        if rc == 0:
            line = 'action: Wait for the resilver to complete.'
            if line in status.splitlines():
                return True
            else:
                return False

    def get_operation_progress(self,zpool):
        operation = None
        progress = {}

        data, err, rc = execute(['zpool', 'status', '-v', zpool])
        if rc == 0:
            scan_data = []
            save = False

            for line in data.splitlines():
                if 'scan:' in line:
                    save = True
                    scan_data.append(line.split('scan:')[1].strip())

                elif 'config:' in line:
                    save = False
                    break

                elif save:
                    scan_data.append(line.strip())

            if scan_data and 'in progress' in scan_data[0]:
                state0 = scan_data[0].split()
                state1 = scan_data[1].split()
                state2 = scan_data[2].split()
                operation = state0[0]
                timestr = scan_data[0].split('since ')[1]
                begin_time = datetime.strptime(timestr, '%a %b %d %H:%M:%S %Y')
                scanned = state1[0]
                to_scan = state1[4]
                rate = state1[6].strip(',')
                if 'scan is slow' in scan_data[1]:
                    time_left = None
                else:
                    time_left = state1[7]
                resilvered = state2[0]
                procent_done = float(state2[2].rstrip('%').replace(',', '.'))

                progress = {'begin_time': begin_time, 'scanned': scanned, 'to_scan': to_scan, 'rate': rate,
                            'time_left': time_left, 'resilvered': resilvered, 'procent_done': procent_done}

        return operation, progress

if __name__ == '__main__':
    import time

    zf = zfs()
    zp = zpool()

    zf.restore_files('archive/video/20.106/190601','videoserver',['/archive/video/20.106/190601/02/0202-00122.mp4',])
