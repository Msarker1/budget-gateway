package com.budget.gateway.verticle;

import com.commons.budget.domain.constant.Operation;
import com.repository.budget.error.BudgetRepositoryErrorResponse;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;

public class BudgetGatewayVerticle extends AbstractVerticle {
	private final Logger logger = LoggerFactory.getLogger(BudgetGatewayVerticle.class);
	@Override
	public void start(Promise<Void> startPromise) throws Exception {
		logger.info("[BudgetGatewayVerticle:start]: starting verticle");
		Router router = Router.router(vertx);
		router.route().handler(BodyHandler.create());
		router.post("/BudgetRepository").handler(this::functionCallBudgetRepository);
		vertx.createHttpServer().requestHandler(router).listen(8080);
		logger.info("[BudgetGatewayVerticle:start]: verticle started");
		startPromise.complete();
	}

	private void functionCallBudgetRepository(RoutingContext ctx) {
		logger.info("[BudgetGatewayVerticle:functionCallBudgetRepository]: processing request");
		Operation op = null;
		try {
			op = Operation.valueOf(ctx.getBodyAsJson().getString("operation"));
		} catch(Exception e) {
			logger.error("[BudgetGatewayVerticle:functionCallBudgetRepository]: incorrect Operation");
			ctx.response().setStatusCode(400).putHeader("content-type", "application/json").end("Incorrect Operation");
		}
		switch(op) {
		case SUBMIT: {
			DeliveryOptions d = new DeliveryOptions();
			d.setHeaders(ctx.request().headers());
			vertx.eventBus().request("BudgetRepository/submitItems", ctx.getBodyAsJson(), d, res -> {
				if(res.succeeded()) {
					logger.info("[BudgetGatewayVerticle:functionCallBudgetRepository]: SUBMIT request Processed.");
					ctx.response().end(res.result().body().toString());
				}else {		
					BudgetRepositoryErrorResponse errResponse = new JsonObject(res.cause().getMessage()).mapTo(BudgetRepositoryErrorResponse.class);
					ctx.response().setStatusCode(errResponse.getErrCode()).end(errResponse.getJson().toString());
				}
			});
			break;
		}	
		case PURCHASE:{
			DeliveryOptions d = new DeliveryOptions();
			d.setHeaders(ctx.request().headers());
			vertx.eventBus().request("BudgetRepository/getPurchases", ctx.getBodyAsJson(), d, res -> {
				if(res.succeeded()) {
					logger.info("[BudgetGatewayVerticle:functionCallBudgetRepository]: PURCHASE request Processed.");
					ctx.response().end(res.result().body().toString());
					
				}else {
					BudgetRepositoryErrorResponse errResponse = new JsonObject(res.cause().getMessage()).mapTo(BudgetRepositoryErrorResponse.class);
					ctx.response().setStatusCode(errResponse.getErrCode()).end(errResponse.getJson().toString());
				}
			});
			break;
		}
		case BALANCE: {
			DeliveryOptions d = new DeliveryOptions();
			d.setHeaders(ctx.request().headers());
			vertx.eventBus().request("BudgetRepository/getBalances", ctx.getBodyAsJson(), d, res -> {
				if(res.succeeded()) {
					logger.info("[BudgetGatewayVerticle:functionCallBudgetRepository]: BALANCE request Processed.");
					ctx.response().end(res.result().body().toString());
				}else {
					BudgetRepositoryErrorResponse errResponse = new JsonObject(res.cause().getMessage()).mapTo(BudgetRepositoryErrorResponse.class);
					ctx.response().setStatusCode(errResponse.getErrCode()).end(errResponse.getErrorMessage());
				}
			});
			break;
		}
		default: {
			ctx.response().setStatusCode(400).putHeader("content-type", "application/json").end("Incorrect Operation");
			break;
		}
		}
	}
}
