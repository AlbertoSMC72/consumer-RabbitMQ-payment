import amqp from "amqplib/callback_api";
import fetch from "node-fetch"; 

async function connect() {
    try {
        await amqp.connect(
            "amqp://34.193.221.88",
            (err: any, conn: amqp.Connection) => {
                if (err) throw new Error(err);
                conn.createChannel((errChanel: any, channel: amqp.Channel) => {
                    if (errChanel) throw new Error(errChanel);
                    channel.assertQueue("cola1"); // Debes especificar la cola
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
                            try {
                                await fetch("http://44.219.166.176:8000/payment", body);
                                console.log("datos enviados");
                                await channel.ack(data); // Acknowledge solo después de que se ha enviado correctamente
                            } catch (err) {
                                console.error("Error al enviar datos:", err);
                                await channel.nack(data); // Reintenta o mueve a una cola de reintentos, según lo deseado
                            }
                        }
                    });
                });
            }
        );
    } catch (err) {
        console.error("Error al conectar:", err);
    }
}

connect();
