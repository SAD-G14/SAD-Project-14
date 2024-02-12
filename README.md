# SAD-Project-14

you can run the project locally with 

```bash

docker compose up --build
```
in this case, you need to specify the host for the server, which will be `localhost`.

or you can use our server. the IP address is hard coded in there.

also, since we had problems with provisioning alerts, we will include
screenshots of how we set it up each time in this doc. these steps have
to be taken after each deploy.

basically, we are using the query defined in the dashboard, when the %usage
of the storage reaches 50%, it will trigger the alert to be sent to a webhook,
(in this case, it is our own server because we could not use telegram API 
on the server).

![](resources/Screenshot%20from%202024-02-12%2011-26-56.png)
![](resources/Screenshot%20from%202024-02-12%2011-27-35.png)
![](resources/Screenshot%20from%202024-02-12%2011-27-56.png)
![](resources/Screenshot%20from%202024-02-12%2011-29-04.png)

