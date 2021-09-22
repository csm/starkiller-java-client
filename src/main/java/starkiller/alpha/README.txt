These classes were in-progress to do client-side routing.

I thought better on the design, and just pushed the dynamic
routing into the server. So, all you need to do on the client
side is connect to ANY node in the cluster, and your message
will be routed correctly. Putting a load balancer or other
service discovery in front of starkiller servers is probably
your best bet.

These classes in the skywalker.alpha package DON'T WORK YET.