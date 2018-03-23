import { Meteor } from 'meteor/meteor';
import { DDPServer } from 'meteor/ddp-server';
import { EJSON } from 'meteor/ejson';
import { Events, RedisPipe } from '../../constants';
import RedisSubscriptionManager from '../../redis/RedisSubscriptionManager';
import { getRedisPusher } from "../../redis/getRedisClient";

const getWriteFence = function (optimistic) {
    if (optimistic && DDPServer._CurrentWriteFence) {
        return DDPServer._CurrentWriteFence.get();
    }
    return null;
};

const dispatchOptimisticEvent = function (fence, channelName, event) {
    const write = fence.beginWrite();
    RedisSubscriptionManager.queue.queueTask(() => {
        RedisSubscriptionManager.process(channelName, event, false);
        write.committed();
    });
};

const dispatchEvents = function (fence, events, channels) {
    if (fence) {
        events.forEach(({ event, dedicatedChannel }) => {
            channels.forEach(channelName => {
                dispatchOptimisticEvent(fence, channelName, event);
            });
            dispatchOptimisticEvent(fence, dedicatedChannel, event);
        });
    }

    Meteor.defer(() => {
        const client = getRedisPusher();
        events.forEach(({ event, dedicatedChannel }) => {
            const message = EJSON.stringify(event);
            channels.forEach(channelName => {
                client.publish(channelName, message);
            });
            client.publish(dedicatedChannel, message);
        });
    });
};


const dispatchUpdate = function (optimistic, collectionName, channels, docIds, fields) {
    const fence = getWriteFence(optimistic);
    const uid = fence ? RedisSubscriptionManager.uid : null;
    const events = docIds.map(docId => {
        const event = {
            [RedisPipe.EVENT]: Events.UPDATE,
            [RedisPipe.FIELDS]: fields,
            [RedisPipe.DOC]: { _id: docId },
            [RedisPipe.UID]: uid,
        };
        const dedicatedChannel = `${collectionName}::${docId}`;
        return { event, dedicatedChannel };
    });
    dispatchEvents(fence, events, channels);
};

const dispatchRemove = function (optimistic, collectionName, channels, docIds) {
    const fence = getWriteFence(optimistic);
    const uid = fence ? RedisSubscriptionManager.uid : null;
    const events = docIds.map(docId => {
        const event = {
            [RedisPipe.EVENT]: Events.REMOVE,
            [RedisPipe.DOC]: { _id: docId },
            [RedisPipe.UID]: uid,
        };
        const dedicatedChannel = `${collectionName}::${docId}`;
        return { event, dedicatedChannel };
    });
    dispatchEvents(fence, events, channels);
};

const dispatchInsert = function (optimistic, collectionName, channels, docId) {
    const fence = getWriteFence(optimistic);
    const uid = fence ? RedisSubscriptionManager.uid : null;
    const event = {
        [RedisPipe.EVENT]: Events.INSERT,
        [RedisPipe.DOC]: { _id: docId },
        [RedisPipe.UID]: uid,
    };
    const dedicatedChannel = `${collectionName}::${docId}`;
    const events = [{ event, dedicatedChannel }];
    dispatchEvents(fence, events, channels);
};

export { dispatchInsert, dispatchUpdate, dispatchRemove };
