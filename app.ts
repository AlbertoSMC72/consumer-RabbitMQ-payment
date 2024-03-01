import amqp from "amqplib/callback_api";

async function connect() {
    try {
        await amqp.connect(
            "amqp://34.193.221.88",
            (err: any, conn: amqp.Connection) => {
                if (err) throw new Error(err);
                conn.createChannel((errChanel: any, channel: amqp.Channel) => {
                    if (errChanel) throw new Error(errChanel);
                    channel.assertQueue();
                    channel.consume("cola1", async (data: amqp.Message | null) => {
                        if (data?.content !== undefined) {
                            console.log(`Recibido nuevo producto: ${data.content}`);
                            const content = data?.content;
                            const parsedContent = JSON.parse(content.toString());
                            const headers = {
                                "Content-Type": "application/json",
                            };
                            const body = {
                                method: "POST",
                                headers,
                                body: JSON.stringify(parsedContent),
                            };
                            console.log(parsedContent);
                            fetch("http://44.219.166.176:8000/payment", body)
                                .then(() => {
                                    console.log("datos enviados");
                                })
                                .catch((err: any) => {
                                    throw new Error(err);
                                }); 
                            await channel.ack(data);
                        }
                    });
                });
            }
        );
    } catch (err: any) {
        throw new Error(err);
    }
}

connect();