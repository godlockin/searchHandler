package searchhandler.rabbitMq.impl;

import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.beans.factory.annotation.Autowired;

@Slf4j
public class RabbitImpl {

    @Autowired
    protected AmqpTemplate rabbitTemplate;

}
