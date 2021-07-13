package io.hstream;

import java.util.List;

public class TestUtils {


    public static void deleteAllSubscriptions(HStreamClient client) {
        // client.listSubscriptions().stream().map(Subscription::getSubscriptionId).forEach(client::deleteSubscription);
        List<Subscription> subscriptions = client.listSubscriptions();
        for(Subscription subscription: subscriptions) {
            System.out.println(subscription);
            System.out.println(subscription.getSubscriptionId());
            client.deleteSubscription(subscription.getSubscriptionId());
        }
    }

}
