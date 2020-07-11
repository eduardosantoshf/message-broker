# FirstCDProject

**Message Broker** capable of linking **producers** and **consumers** through a common **PubSub** protocol and three different serialization mechanisms (**XML**, **JSON** and **Pickle**) + **middleware** that **abstracts** producers and consumers from the entire communication process.

## Course
This project was developed under the [Distributed Computing](https://www.ua.pt/en/uc/12273) course of [University of Aveiro](https://www.ua.pt/).

## How to run
This implementation supports communication between **N** producers and **N** consumers through the middleware, using different **topics** and **subtopics**.

Topic used in this implementation:
* temp
* msg
* weather

Run **broker.py**:
```console
$ python3 broker.py
````

Run **producer.py** (1 - N producers):
```console
$ python3 producer.py [--type TYPE]
```

Run **consumers.py** (1 - N consumers):
```console
$ python3 consumers.py [--type TYPE] [--length LENGTH]
```

TYPE - type of producer: [temp, msg, weather]; LENGTH - number of messages to be sent (default = 10)

## Authors
* **Eduardo Santos**: [eduardosantoshf](https://github.com/eduardosantoshf)
* **Pedro Bastos**: [bastos-01](https://github.com/bastos-01)
