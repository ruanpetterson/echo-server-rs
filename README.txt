$ wrk -t10 -c1000 -d10s http://localhost:1337
Running 10s test @ http://localhost:1337
  10 threads and 1000 connections
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency    10.05ms    8.54ms  40.69ms   99.35%
    Req/Sec    10.02k     1.58k   27.74k    82.90%
  997652 requests in 10.03s, 96.09MB read
Requests/sec:  99444.66
Transfer/sec:      9.58MB
