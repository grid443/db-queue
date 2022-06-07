package com.grid.daaq;

public interface Publisher<PayloadType, SessionType> {
    Receipt publish(SessionType session, MessageHeaders headers, PayloadType payload);
}
