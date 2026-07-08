use crate::{
    AppState,
    model::{DeliveryEvent, DeliveryEventBatch},
};

pub(crate) fn publish_delivery_event(state: &AppState, event: DeliveryEvent) {
    state
        .aggregates
        .apply_delivery_events(std::slice::from_ref(&event));
    if state.realtime_fanout.publish(vec![event.clone()]) {
        return;
    }
    if state.events.receiver_count() == 0 {
        return;
    }
    let _ = state.events.send(vec![event]);
}

pub(crate) fn publish_delivery_events(state: &AppState, events: DeliveryEventBatch) {
    if events.is_empty() {
        return;
    }
    state.aggregates.apply_delivery_events(&events);
    if state.realtime_fanout.publish(events.clone()) {
        return;
    }
    if state.events.receiver_count() > 0 {
        let _ = state.events.send(events);
    }
}
