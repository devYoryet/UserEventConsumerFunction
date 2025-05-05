package com.userrolemgmt;

import com.microsoft.azure.functions.annotation.*;
import com.microsoft.azure.functions.*;
import java.util.logging.Logger;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class UserEventConsumerFunction {

    private static final Gson gson = new Gson();

    @FunctionName("UserEventConsumer")
    public void run(
            @EventGridTrigger(name = "event") String content,
            final ExecutionContext context) {

        Logger logger = context.getLogger();
        logger.info("User Event Grid trigger function processed an event: " + content);

        try {
            // Parsear el evento
            JsonObject eventJson = JsonParser.parseString(content).getAsJsonObject();
            String eventType = eventJson.get("eventType").getAsString();

            // Procesar según el tipo de evento
            if (eventType.equals("UserCreated")) {
                // Lógica específica para cuando se crea un usuario
                logger.info("Procesando evento de creación de usuario");
                // Por ejemplo, podrías enviar una notificación de bienvenida
            } else if (eventType.equals("UserUpdated")) {
                // Lógica específica para cuando se actualiza un usuario
                logger.info("Procesando evento de actualización de usuario");
            } else if (eventType.equals("UserDeleted")) {
                // Lógica específica para cuando se elimina un usuario
                logger.info("Procesando evento de eliminación de usuario");
            }

            // Puedes agregar más tipos de eventos según sea necesario

        } catch (Exception e) {
            logger.severe("Error procesando el evento: " + e.getMessage());
        }
    }
}