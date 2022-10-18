package info.bitrich.xchangestream.okcoin.examples;

import info.bitrich.xchangestream.core.StreamingExchangeFactory;
import info.bitrich.xchangestream.okcoin.OkxStreamingExchange;
import info.bitrich.xchangestream.okcoin.dto.okx.enums.OkxInstType;
import org.knowm.xchange.ExchangeSpecification;
import org.knowm.xchange.currency.CurrencyPair;
import org.knowm.xchange.instrument.Instrument;

public class OkxStreamingExchangeExample {
    public static void main(String[] args) throws InterruptedException {
        ExchangeSpecification spec = getExchangeSpecification();

        OkxStreamingExchange exchange = (OkxStreamingExchange) StreamingExchangeFactory.INSTANCE.createExchange(spec);
        exchange.connect().blockingAwait();

        Instrument instrument = CurrencyPair.BTC_USDT;
        exchange.getStreamingMarketDataService()
                .getTrades(instrument)
                .forEach(System.out::println);

        exchange.getStreamingTradeService()
//                .getUserTrades(instrument, OkxInstType.SPOT, OkexAdapters.adaptCurrencyPairId(instrument), OkexAdapters.adaptInstrumentId(instrument))
                .getUserTrades(instrument, OkxInstType.ANY)
                .forEach(System.out::println);

        Thread.sleep(1000000);
    }

    private static ExchangeSpecification getExchangeSpecification() {
        // Enter your authentication details here to run private endpoint tests
        final String API_KEY = System.getenv("okx_apikey");
        final String SECRET_KEY = System.getenv("okx_secretkey");
        final String PASSPHRASE = System.getenv("okx_passphrase");

        ExchangeSpecification spec = new OkxStreamingExchange().getDefaultExchangeSpecification();
        spec.setApiKey(API_KEY);
        spec.setSecretKey(SECRET_KEY);
        spec.setExchangeSpecificParametersItem("passphrase", PASSPHRASE);

//        spec.setExchangeSpecificParametersItem(OkxStreamingExchange.USE_SANDBOX, true);
        spec.setProxyHost("127.0.0.1");
        spec.setProxyPort(8118);
        spec.setExchangeSpecificParametersItem(OkxStreamingExchange.SOCKS_PROXY_HOST, "127.0.0.1");
        spec.setExchangeSpecificParametersItem(OkxStreamingExchange.SOCKS_PROXY_PORT, 8119);

        return spec;
    }
}
