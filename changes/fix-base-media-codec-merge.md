Fixed

- The server builds again after the codec routes and the media routes landed together: video ingress reads its request through the fenced accessor, and the exchange path refuses search and video bodies the way it refuses the other media protocols, which their own routes serve.
