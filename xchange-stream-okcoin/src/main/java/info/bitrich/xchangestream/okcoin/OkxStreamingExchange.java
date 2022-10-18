package info.bitrich.xchangestream.okcoin;

import com.fasterxml.jackson.core.JsonProcessingException;
import info.bitrich.xchangestream.core.ProductSubscription;
import info.bitrich.xchangestream.core.StreamingExchange;
import info.bitrich.xchangestream.core.StreamingMarketDataService;
import info.bitrich.xchangestream.core.StreamingTradeService;
import io.reactivex.Completable;
import io.reactivex.CompletableSource;
import org.apache.commons.lang3.StringUtils;
import org.knowm.xchange.ExchangeSpecification;
import org.knowm.xchange.exceptions.NotYetImplementedForExchangeException;
import org.knowm.xchange.okex.v5.OkexExchange;

import java.util.ArrayList;
import java.util.List;

public class OkxStreamingExchange extends OkexExchange implements StreamingExchange {
    // Production URIs
    public static final String WS_PUBLIC_CHANNEL_URI = "wss://ws.okx.com:8443/ws/v5/public";
    public static final String WS_PRIVATE_CHANNEL_URI = "wss://ws.okx.com:8443/ws/v5/private";

    public static final String AWS_WS_PUBLIC_CHANNEL_URI = "wss://wsaws.okx.com:8443/ws/v5/public";
    public static final String AWS_WS_PRIVATE_CHANNEL_URI = "wss://wsaws.okx.com:8443/ws/v5/private";

    // Demo(Sandbox) URIs
    public static final String SANDBOX_WS_PUBLIC_CHANNEL_URI = "wss://wspap.okx.com:8443/ws/v5/public?brokerId=9999";
    public static final String SANDBOX_WS_PRIVATE_CHANNEL_URI = "wss://wspap.okx.com:8443/ws/v5/private?brokerId=9999";

    private OkxStreamingService publicStreamingService;
    private OkxStreamingService privateStreamingService;

    private OkxStreamingMarketDataService streamingMarketDataService;
    private OkxStreamingTradeService streamingTradeService;

    public OkxStreamingExchange() {}

    @Override
    public Completable connect(ProductSubscription... args) {
        ExchangeSpecification exchangeSpec = getExchangeSpecification();
        List<Completable> completables = new ArrayList<>();

        String publicApiUrl = getPublicApiUrl();
        this.publicStreamingService = new OkxStreamingService(publicApiUrl, exchangeSpec);
        applyStreamingSpecification(exchangeSpec, this.publicStreamingService);
        this.streamingMarketDataService = new OkxStreamingMarketDataService(this.publicStreamingService);
        completables.add(publicStreamingService.connect());

        if (StringUtils.isNotEmpty(exchangeSpec.getApiKey())) {
            String privateApiUrl = getPrivateApiUrl();
            this.privateStreamingService = new OkxStreamingService(privateApiUrl, exchangeSpec);
            applyStreamingSpecification(exchangeSpec, this.privateStreamingService);
            Completable pCompletable = privateStreamingService.connect();
            completables.add(pCompletable.andThen(
                (CompletableSource)
                    (completable) -> {
                        try {
                            privateStreamingService.login();
                            completable.onComplete();
                        } catch (JsonProcessingException e) {
                            completable.onError(e);
                        }
            }));
            this.streamingTradeService = new OkxStreamingTradeService(this.privateStreamingService);
        }

        return Completable.concat(completables);
    }

    private String getPublicApiUrl() {
        String apiUrl;
        ExchangeSpecification exchangeSpec = getExchangeSpecification();
        if (exchangeSpec.getOverrideWebsocketApiUri() != null) {
            return exchangeSpec.getOverrideWebsocketApiUri();
        }

        boolean useSandbox =
                Boolean.TRUE.equals(
                        exchangeSpecification.getExchangeSpecificParametersItem(USE_SANDBOX)
                );
        boolean userAws =
                Boolean.TRUE.equals(
                        exchangeSpecification.getExchangeSpecificParametersItem(Parameters.PARAM_USE_AWS)
                );
        if (useSandbox) {
            apiUrl = SANDBOX_WS_PUBLIC_CHANNEL_URI;
        } else {
            apiUrl = userAws ? AWS_WS_PUBLIC_CHANNEL_URI : WS_PUBLIC_CHANNEL_URI;
        }
        return apiUrl;
    }

    private String getPrivateApiUrl() {
        String apiUrl;
        ExchangeSpecification exchangeSpec = getExchangeSpecification();
        if (exchangeSpec.getOverrideWebsocketApiUri() != null) {
            return exchangeSpec.getOverrideWebsocketApiUri();
        }

        boolean useSandbox =
                Boolean.TRUE.equals(
                        exchangeSpecification.getExchangeSpecificParametersItem(USE_SANDBOX)
                );
        boolean userAws =
                Boolean.TRUE.equals(
                        exchangeSpecification.getExchangeSpecificParametersItem(Parameters.PARAM_USE_AWS)
                );
        if (useSandbox) {
            apiUrl = SANDBOX_WS_PRIVATE_CHANNEL_URI;
        } else {
            apiUrl = userAws ? AWS_WS_PRIVATE_CHANNEL_URI : WS_PRIVATE_CHANNEL_URI;
        }
        return apiUrl;
    }

    @Override
    public Completable disconnect() {
        List<Completable> completables = new ArrayList<>();

        completables.add(publicStreamingService.disconnect());
        this.publicStreamingService = null;
        this.streamingMarketDataService = null;

        if (streamingTradeService != null) {
            this.streamingTradeService = null;
        }
        if (privateStreamingService != null) {
            completables.add(privateStreamingService.disconnect());
        }
        return Completable.concat(completables);
    }

    @Override
    public boolean isAlive() {
        return (publicStreamingService != null && publicStreamingService.isSocketOpen())
                && (privateStreamingService == null || privateStreamingService.isSocketOpen());
    }

    @Override
    public StreamingMarketDataService getStreamingMarketDataService() {
        return streamingMarketDataService;
    }

    @Override
    public StreamingTradeService getStreamingTradeService() {
        return streamingTradeService;
    }

    @Override
    public void useCompressedMessages(boolean compressedMessages) {
        throw new NotYetImplementedForExchangeException("useCompressedMessage");
    }
}
