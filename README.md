# MemcLoad

Multiprocessing implementation of loading (parsing, validating) gziped files (tracer logs of installed applications) and saving according protobuf messages into memcache servers (per dev type).

## Purpose:
- Make research the operations of an existing application `memc_load_orig.py` in order to improve its performance.
- Implement improvements `memc_load.py` and report results.

### What we have:

- There are four types of records, each must be stored on its `memcache` server as follows:
    - idfa -> 127.0.0.1:33013
    - gaid -> 127.0.0.1:33014
    - adid -> 127.0.0.1:33015
    - dvid" -> 127.0.0.1:33016

- Files with test data:
    - https://cloud.mail.ru/public/2hZL/Ko9s8R9TA
    - https://cloud.mail.ru/public/DzSX/oj8RxGX1A
    - https://cloud.mail.ru/public/LoDo/SfsPEzoGc

- File format:
    > $ zcat 20170929000000.tsv.gz | head -1
    ```
    idfa	e7e1a50c0ec2747ca56cd9e1558c0d7c	67.7835424444	-22.8044005471	7942,8519,4232,3032,4766,9283,5682,155,5779,2260,3624,1358,2432,1212,528,8182,9061,9628,2055,4821,3550,4964,6924,6737,3784,5428,6980,8137,2129,8751,3000,5495,5674,3023,818,2864,8250,768,6931,3493,3749,8053,8815,8448,8757,272,5951,2831,7186,157,1629,2021,3338,9020,6679,8679,1477,7488,3751,7399,8556,5500,5333,3873,7070,3018,2734,4273,3723,4528,4657,4014
    ```
- Record types statistics:
    > $ zcat 20170929000000.tsv.gz | cut -f1 | sort | uniq -c

    ```
    855327 adid
    856647 dvid
    855036 gaid
    855985 idfa
    ```
    => test files contain an equal distribution of data by type - **very good!**

    *in this way (having other things being equal) we can rely on a uniform loading balance on memcache servers, which we will use.*

- Performance measurement results and bottleneck identification in code.
    
    > $ kernprof -l -v -o mc_original.profile memc_load_orig.py --pattern=data/appsinstalled/h1024_*.tsv.gz

*{n} records from each test files were selected for testing. n: 1024 (for code profiling), 100000 (for result preformance testing)*
```
Wrote profile results to memc_load_orig.py.lprof
Timer unit: 1e-06 s

Total time: 5.04934 s
File: memc_load_orig.py
Function: insert_appsinstalled at line 31

Line #      Hits         Time  Per Hit   % Time  Line Contents
==============================================================
    31                                           @profile
    32                                           def insert_appsinstalled(memc_addr, appsinstalled, dry_run=False):
    33      3072      65809.0     21.4      1.3      ua = appsinstalled_pb2.UserApps()
    34      3072      31875.0     10.4      0.6      ua.lat = appsinstalled.lat
    35      3072      11343.0      3.7      0.2      ua.lon = appsinstalled.lon
    36      3072      31840.0     10.4      0.6      key = "%s:%s" % (appsinstalled.dev_type, appsinstalled.dev_id)
    37      3072     101976.0     33.2      2.0      ua.apps.extend(appsinstalled.apps)
    38      3072     120343.0     39.2      2.4      packed = ua.SerializeToString()
    39                                               # @TODO persistent connection
    40                                               # @TODO retry and timeouts!
    41      3072       9479.0      3.1      0.2      try:
    42      3072       7074.0      2.3      0.1          if dry_run:
    43                                                       logging.debug("%s - %s -> %s" % (memc_addr, key, str(ua).replace("\n", " ")))
    44                                                   else:
    45      3072     993539.0    323.4     19.7              memc = memcache.Client([memc_addr])
    46      3072    3651602.0   1188.7     72.3              memc.set(key, packed)
    47                                               except Exception as e:
    48                                                   logging.exception("Cannot write to memc %s: %s" % (memc_addr, e))
    49                                                   return False
    50      3072      24464.0      8.0      0.5      return True

Total time: 7.3008 s
File: memc_load_orig.py
Function: main at line 72

Line #      Hits         Time  Per Hit   % Time  Line Contents
==============================================================
    72                                           @profile
    73                                           def main(options):
    74         1          4.0      4.0      0.0      device_memc = {
    75         1          9.0      9.0      0.0          b"idfa": options.idfa,
    76         1          3.0      3.0      0.0          b"gaid": options.gaid,
    77         1          2.0      2.0      0.0          b"adid": options.adid,
    78         1          2.0      2.0      0.0          b"dvid": options.dvid,
    79                                               }
    80         4       4553.0   1138.2      0.1      for fn in glob.iglob(options.pattern):
    81         3         14.0      4.7      0.0          processed = errors = 0
    82         3       1871.0    623.7      0.0          logging.info('Processing %s' % fn)
    83         3        794.0    264.7      0.0          fd = gzip.open(fn, 'rb')
    84      3075     148746.0     48.4      2.0          for line in fd:
    85      3072      17740.0      5.8      0.2              line = line.strip()
    86      3072       9532.0      3.1      0.1              if not line:
    87                                                           continue
    88      3072     519062.0    169.0      7.1              appsinstalled = parse_appsinstalled(line)
    89      3072      15573.0      5.1      0.2              if not appsinstalled:
    90                                                           logging.error("Invalid appsinstalled record: %s" % line)
    91                                                           errors += 1
    92                                                           continue
    93      3072      23102.0      7.5      0.3              memc_addr = device_memc.get(appsinstalled.dev_type)
    94      3072      10370.0      3.4      0.1              if not memc_addr:
    95                                                           errors += 1
    96                                                           logging.error("Unknow device type: %s" % appsinstalled.dev_type)
    97                                                           continue
    98      3072    6494924.0   2114.2     89.0              ok = insert_appsinstalled(memc_addr, appsinstalled, options.dry)
    99      3072      21467.0      7.0      0.3              if ok:
   100      3072      17267.0      5.6      0.2                  processed += 1
   101                                                       else:
   102                                                           errors += 1
   103         3         14.0      4.7      0.0          if not processed:
   104                                                       fd.close()
   105                                                       dot_rename(fn)
   106                                                       continue
   107                                           
   108         3         18.0      6.0      0.0          err_rate = float(errors) / processed
   109         3         11.0      3.7      0.0          if err_rate < NORMAL_ERR_RATE:
   110         3       8140.0   2713.3      0.1              logging.info("Acceptable error rate (%s). Successfull load" % err_rate)
   111                                                   else:
   112                                                       logging.error("High error rate (%s > %s). Failed load" % (err_rate, NORMAL_ERR_RATE))
   113         3        354.0    118.0      0.0          fd.close()
   114         3       7233.0   2411.0      0.1          dot_rename(fn)
```

## Decision:

Based on the data obtained, the following improvements are applyed:
- files are processed in parallel by *worker* using process pool;
- each *worker* within her process launches four threads (per each dev type) to save records into corresponding memcache server concurrently;
- сommunication through queues;
- apply message buffering to store messages in batches.
- remove all unnecessary conversions (from bytes to string and vice versa);
- replace lists with tuples, etc. on trifles.

## Instalation and usage:

- Depends:
    - OS: Linux
    - Service: memchache

    > pip install -r requirements.txt

    > protoc  --python_out=. ./appsinstalled.proto

    > ./reset.sh

    > python  memc_load.py [OPTIONS]

```      
	-t, --test       run protobuf test
	-l, --log        set log path
    -w, --wokers     workers count
	--dry            debug mode
	--pattern        log path, default: "/data/appsinstalled/*.tsv.gz"
	--idfa           idfa memcache address, default value: "127.0.0.1:33013"
	--gaid           gaid memcache address, default value: "127.0.0.1:33014"
	--adid           adid memcache address, default value: "127.0.0.1:33015"
	--dvid           dvid memcache address, default value: "127.0.0.1:33016"
```

## Timed results:

- original:
```
Command being timed: "python memc_load_orig.py --pattern=data/appsinstalled/h100000_*.tsv.gz -l memc_load.log"
	User time (seconds): 235.09
	System time (seconds): 146.91
	Percent of CPU this job got: 71%
	Elapsed (wall clock) time (h:mm:ss or m:ss): 8:52.62
	Average shared text size (kbytes): 0
	Average unshared data size (kbytes): 0
	Average stack size (kbytes): 0
	Average total size (kbytes): 0
	Maximum resident set size (kbytes): 17696
	Average resident set size (kbytes): 0
	Major (requiring I/O) page faults: 0
	Minor (reclaiming a frame) page faults: 2283
	Voluntary context switches: 248676
	Involuntary context switches: 289370
	Swaps: 0
	File system inputs: 264
	File system outputs: 32
	Socket messages sent: 0
	Socket messages received: 0
	Signals delivered: 0
	Page size (bytes): 4096
	Exit status: 0
```
- after improvements:
```
Command being timed: "python memc_load.py --pattern=data/appsinstalled/h100000_*.tsv.gz -l memc_load.log"

    User time (seconds): 60.04
	System time (seconds): 5.29
	Percent of CPU this job got: 146%
	Elapsed (wall clock) time (h:mm:ss or m:ss): 0:44.50
	Average shared text size (kbytes): 0
	Average unshared data size (kbytes): 0
	Average stack size (kbytes): 0
	Average total size (kbytes): 0
	Maximum resident set size (kbytes): 37712
	Average resident set size (kbytes): 0
	Major (requiring I/O) page faults: 0
	Minor (reclaiming a frame) page faults: 26201
	Voluntary context switches: 42724
	Involuntary context switches: 31839
	Swaps: 0
	File system inputs: 2344
	File system outputs: 16
	Socket messages sent: 0
	Socket messages received: 0
	Signals delivered: 0
	Page size (bytes): 4096
	Exit status: 0
```
