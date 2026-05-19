$ wrk -t10 -c1000 -d10s http://localhost:1337
Running 10s test @ http://localhost:1337
  10 threads and 1000 connections
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency     7.80ms    1.75ms  32.95ms   92.90%
    Req/Sec    12.90k     2.31k   46.98k    89.30%
  1284477 requests in 10.02s, 123.72MB read
Requests/sec: 128164.85
Transfer/sec:     12.34MB
