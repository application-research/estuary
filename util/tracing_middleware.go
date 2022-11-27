package util

import (
	"context"

	"github.com/labstack/echo/v4"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
	"go.opentelemetry.io/otel/trace"
)

func TracingMiddleware(tcr trace.Tracer) echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			r := c.Request()

			attrs := []attribute.KeyValue{
				semconv.HTTPMethodKey.String(r.Method),
				semconv.HTTPRouteKey.String(r.URL.Path),
				semconv.HTTPClientIPKey.String(r.RemoteAddr),
				semconv.HTTPRequestContentLengthKey.Int64(c.Request().ContentLength),
			}

			if reqid := r.Header.Get("EstClientReqID"); reqid != "" {
				if len(reqid) > 64 {
					reqid = reqid[:64]
				}
				attrs = append(attrs, attribute.String("ClientReqID", reqid))
			}

			tctx, span := tcr.Start(context.Background(),
				"HTTP "+r.Method+" "+c.Path(),
				trace.WithAttributes(attrs...),
			)
			defer span.End()

			r = r.WithContext(tctx)
			c.SetRequest(r)

			err := next(c)
			if err != nil {
				span.SetStatus(codes.Error, err.Error())
				span.RecordError(err)
			} else {
				span.SetStatus(codes.Ok, "")
			}

			span.SetAttributes(
				semconv.HTTPStatusCodeKey.Int(c.Response().Status),
				semconv.HTTPResponseContentLengthKey.Int64(c.Response().Size),
			)
			return err
		}
	}
}
