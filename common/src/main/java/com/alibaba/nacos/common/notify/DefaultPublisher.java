/*
 * Copyright 1999-2018 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.nacos.common.notify;

import com.alibaba.nacos.common.notify.listener.Subscriber;
import com.alibaba.nacos.common.utils.CollectionUtils;
import com.alibaba.nacos.common.utils.ConcurrentHashSet;
import com.alibaba.nacos.common.utils.ThreadUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import static com.alibaba.nacos.common.notify.NotifyCenter.ringBufferSize;

/**
 * The default event publisher implementation.
 *
 * <p>Internally, use {@link ArrayBlockingQueue <Event/>} as a message staging queue.
 *
 * @author <a href="mailto:liaochuntao@live.com">liaochuntao</a>
 * @author zongtanghu
 */
public class DefaultPublisher extends Thread implements EventPublisher {

    protected static final Logger LOGGER = LoggerFactory.getLogger(NotifyCenter.class);

    private volatile boolean initialized = false;

    //定义跳出终止线程的共享变量
    private volatile boolean shutdown = false;

    private Class<? extends Event> eventType;

    protected final ConcurrentHashSet<Subscriber> subscribers = new ConcurrentHashSet<Subscriber>();

    private int queueMaxSize = -1;

    private BlockingQueue<Event> queue;

    protected volatile Long lastEventSequence = -1L;

    private static final AtomicReferenceFieldUpdater<DefaultPublisher, Long> UPDATER = AtomicReferenceFieldUpdater
            .newUpdater(DefaultPublisher.class, Long.class, "lastEventSequence");

    @Override
    public void init(Class<? extends Event> type, int bufferSize) {
        //设置为守护线程
        setDaemon(true);
        //定义线程的名字和事件挂钩
        setName("nacos.publisher-" + type.getName());
        this.eventType = type;
        //阻塞队列最大大小
        this.queueMaxSize = bufferSize;
        //定义阻塞队列大小
        this.queue = new ArrayBlockingQueue<Event>(bufferSize);
        //启动线程
        start();
    }

    public ConcurrentHashSet<Subscriber> getSubscribers() {
        return subscribers;
    }

    @Override
    public synchronized void start() {
        if (!initialized) {
            // start just called once
            super.start();
            if (queueMaxSize == -1) {
                queueMaxSize = ringBufferSize;
            }
            //启动线程，设置标记初始化为true，开始执行thread的run方法
            initialized = true;
        }
    }

    @Override
    public long currentEventSize() {
        return queue.size();
    }

    @Override
    public void run() {
        //执行run方法
        openEventHandler();
    }

    void openEventHandler() {
        try {

            // This variable is defined to resolve the problem which message overstock in the queue.
            //定义变量是为了解决队列消息积压问题
            int waitTimes = 60;
            // To ensure that messages are not lost, enable EventHandler when
            // waiting for the first Subscriber to register
            //等待第一个注册的订阅者，如果一直没有注册，最多等待60秒，开始从队列中读取消息
            for (; ; ) {
                if (shutdown || hasSubscriber() || waitTimes <= 0) {
                    break;
                }
                ThreadUtils.sleep(1000L);
                waitTimes--;
            }

            for (; ; ) {
                if (shutdown) {
                    break;
                }
                //不断从队列读取事件消息
                final Event event = queue.take();
                //接收事件处理
                receiveEvent(event);
                UPDATER.compareAndSet(this, lastEventSequence, Math.max(lastEventSequence, event.sequence()));
            }
        } catch (Throwable ex) {
            LOGGER.error("Event listener exception : {}", ex);
        }
    }

    private boolean hasSubscriber() {
        return CollectionUtils.isNotEmpty(subscribers);
    }

    @Override
    public void addSubscriber(Subscriber subscriber) {
        subscribers.add(subscriber);
    }

    @Override
    public void removeSubscriber(Subscriber subscriber) {
        subscribers.remove(subscriber);
    }

    @Override
    //发布事件，
    public boolean publish(Event event) {
        checkIsStart();
        //往队列中添加事件
        boolean success = this.queue.offer(event);
        //如果添加不成功
        if (!success) {
            LOGGER.warn("Unable to plug in due to interruption, synchronize sending time, event : {}", event);
            //直接执行事件处理
            receiveEvent(event);
            return true;
        }
        return true;
    }

    void checkIsStart() {
        if (!initialized) {
            throw new IllegalStateException("Publisher does not start");
        }
    }

    @Override
    public void shutdown() {
        this.shutdown = true;
        this.queue.clear();
    }

    public boolean isInitialized() {
        return initialized;
    }

    /**
     * Receive and notifySubscriber to process the event.
     *
     * @param event {@link Event}.
     */
    void receiveEvent(Event event) {
        final long currentEventSequence = event.sequence();
        //订阅者从哪里来，参考 NacosNamingService的init方法
        // Notification single event listener
        for (Subscriber subscriber : subscribers) {
            // Whether to ignore expiration events
            if (subscriber.ignoreExpireEvent() && lastEventSequence > currentEventSequence) {
                LOGGER.debug("[NotifyCenter] the {} is unacceptable to this subscriber, because had expire",
                        event.getClass());
                continue;
            }

            // Because unifying smartSubscriber and subscriber, so here need to think of compatibility.
            // Remove original judge part of codes.
            notifySubscriber(subscriber, event);
        }
    }

    @Override
    public void notifySubscriber(final Subscriber subscriber, final Event event) {

        LOGGER.debug("[NotifyCenter] the {} will received by {}", event, subscriber);
        //组装处理事件
        final Runnable job = () -> subscriber.onEvent(event);
        //获取订阅者的线程池
        final Executor executor = subscriber.executor();

        if (executor != null) {
            //线程池执行
            executor.execute(job);
        } else {
            try {
                job.run();
            } catch (Throwable e) {
                LOGGER.error("Event callback exception: ", e);
            }
        }
    }
}
