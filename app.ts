import amqp from "amqplib/callback_api";
import fetch from "node-fetch"; 

async function connect() {
    try {
        await amqp.connect(
            "amqp://3.216.68.220",
            (err: any, conn: amqp.Connection) => {
                if (err) throw new Error(err);
                conn.createChannel((errChanel: any, channel: amqp.Channel) => {
                    if (errChanel) throw new Error(errChanel);
                    channel.assertQueue("cola_analisis"); 
                    channel.consume("cola_analisis", async (data: amqp.Message | null) => {
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
                                await fetch("http://3.212.95.23:3000/api/analisi", body);
                                console.log("datos enviados");
                                await channel.ack(data); 
                            } catch (err) {
                                console.error("Error al enviar datos:", err);
                                await channel.nack(data); 
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
