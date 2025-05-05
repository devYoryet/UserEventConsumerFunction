package com.userrolemgmt;

import com.microsoft.azure.functions.annotation.*;
import com.microsoft.azure.functions.*;
import java.util.logging.Logger;
import java.util.logging.Level;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.userrolemgmt.dao.EventDAO;

public class UserEventConsumerFunction {

    private static final Gson gson = new Gson();
    private EventDAO eventDAO;

    public UserEventConsumerFunction() {
        this.eventDAO = new EventDAO();
    }

    @FunctionName("UserEventConsumer")
    public void run(
            @EventGridTrigger(name = "event") String content,
            final ExecutionContext context) {

        Logger logger = context.getLogger();
        logger.info("User Event Grid trigger function processed an event: " + content);

        long eventId = -1;

        try {
            // Parsear el evento
            JsonObject eventJson = JsonParser.parseString(content).getAsJsonObject();
            String eventType = eventJson.get("eventType").getAsString();
            String subject = eventJson.get("subject").getAsString();

            logger.info("Evento parseado correctamente - Tipo: " + eventType + ", Asunto: " + subject);

            try {
                // Registrar el evento en el Event Store
                logger.info("Intentando registrar evento en Event Store");
                eventId = eventDAO.storeEvent(eventType, subject, content);
                logger.info("Evento registrado exitosamente en Event Store con ID: " + eventId);
            } catch (Exception e) {
                logger.log(Level.SEVERE, "Error al registrar evento en Event Store: " + e.getMessage(), e);
                return; // No podemos devolver un HTTP status en este método void
            }

            // Procesar según el tipo de evento
            if (eventType.equals("UserCreated")) {
                try {
                    // Lógica específica para cuando se crea un usuario
                    logger.info("Procesando evento de creación de usuario");

                    // Simular envío de notificación
                    logger.info("Enviando email de bienvenida al nuevo usuario");

                    // Simular actualización de caché
                    logger.info("Actualizando caché de usuarios");

                    // Marca el evento como procesado exitosamente
                    eventDAO.markEventProcessed(eventId, true, null);
                    logger.info("Evento de creación de usuario procesado exitosamente");
                } catch (Exception e) {
                    logger.log(Level.SEVERE, "Error al procesar evento UserCreated: " + e.getMessage(), e);
                    if (eventId != -1) {
                        try {
                            eventDAO.markEventProcessed(eventId, false, e.getMessage());
                        } catch (Exception ex) {
                            logger.log(Level.SEVERE, "Error al marcar evento como fallido: " + ex.getMessage(), ex);
                        }
                    }
                    return; // Terminar la función
                }
            } else if (eventType.equals("UserUpdated")) {
                try {
                    // Lógica para usuarios actualizados...
                    logger.info("Procesando evento de actualización de usuario");
                    eventDAO.markEventProcessed(eventId, true, null);
                    logger.info("Evento de actualización de usuario procesado exitosamente");
                } catch (Exception e) {
                    logger.log(Level.SEVERE, "Error al procesar evento UserUpdated: " + e.getMessage(), e);
                    if (eventId != -1) {
                        try {
                            eventDAO.markEventProcessed(eventId, false, e.getMessage());
                        } catch (Exception ex) {
                            logger.log(Level.SEVERE, "Error al marcar evento como fallido: " + ex.getMessage(), ex);
                        }
                    }
                    return; // Terminar la función
                }
            } else if (eventType.equals("UserDeleted")) {
                try {
                    // Lógica para usuarios eliminados...
                    logger.info("Procesando evento de eliminación de usuario");
                    eventDAO.markEventProcessed(eventId, true, null);
                    logger.info("Evento de eliminación de usuario procesado exitosamente");
                } catch (Exception e) {
                    logger.log(Level.SEVERE, "Error al procesar evento UserDeleted: " + e.getMessage(), e);
                    if (eventId != -1) {
                        try {
                            eventDAO.markEventProcessed(eventId, false, e.getMessage());
                        } catch (Exception ex) {
                            logger.log(Level.SEVERE, "Error al marcar evento como fallido: " + ex.getMessage(), ex);
                        }
                    }
                    return; // Terminar la función
                }
            }

            // Si todo salió bien
            logger.info("Evento procesado exitosamente");

        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error general procesando el evento: " + e.getMessage(), e);

            if (eventId != -1) {
                try {
                    eventDAO.markEventProcessed(eventId, false, "Error general: " + e.getMessage());
                } catch (Exception ex) {
                    logger.log(Level.SEVERE, "Error al marcar evento como fallido: " + ex.getMessage(), ex);
                }
            }
        }
    }
}