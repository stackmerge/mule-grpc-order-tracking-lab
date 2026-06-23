# MuleSoft gRPC Order Tracking Lab

## Mule 4.11+ | APIkit for gRPC | Protobuf | Runtime Fabric on EKS | HTTP/2 Ingress

This lab demonstrates a complete end-to-end gRPC API implementation in MuleSoft.

You will design a Protobuf contract, publish it to Anypoint Exchange, scaffold a Mule implementation using APIkit for gRPC, implement unary and server-streaming RPC methods, test the service with `grpcurl`, deploy it to Runtime Fabric on Amazon EKS, configure HTTP/2-capable ingress, and validate the public gRPC endpoint.

---

## Table of Contents

1. [Lab Objective](#lab-objective)
2. [What You Will Build](#what-you-will-build)
3. [Architecture](#architecture)
4. [Important Deployment Positioning](#important-deployment-positioning)
5. [gRPC, HTTP/2, and ALPN](#grpc-http2-and-alpn)
6. [Prerequisites](#prerequisites)
7. [Suggested Repository Structure](#suggested-repository-structure)
8. [Create the Protobuf Contract](#create-the-protobuf-contract)
9. [Design, Govern, and Publish in Anypoint Code Builder](#design-govern-and-publish-in-anypoint-code-builder)
10. [Scaffold the Mule Application with APIkit for gRPC](#scaffold-the-mule-application-with-apikit-for-grpc)
11. [Understand the Generated Mule Project](#understand-the-generated-mule-project)
12. [Configure HTTP/2 in Mule](#configure-http2-in-mule)
13. [Implement the Unary RPC](#implement-the-unary-rpc)
14. [Implement the Server-Streaming RPC](#implement-the-server-streaming-rpc)
15. [Build and Run Locally](#build-and-run-locally)
16. [Test Locally with grpcurl](#test-locally-with-grpcurl)
17. [Prepare Runtime Fabric on EKS](#prepare-runtime-fabric-on-eks)
18. [Configure HTTP/2 Ingress with NGINX](#configure-http2-ingress-with-nginx)
19. [Create and Synchronize the TLS Secret](#create-and-synchronize-the-tls-secret)
20. [Create the Runtime Fabric HTTPRouteTemplate](#create-the-runtime-fabric-httproutetemplate)
21. [Deploy the Mule App to Runtime Fabric](#deploy-the-mule-app-to-runtime-fabric)
22. [Verify Kubernetes Resources](#verify-kubernetes-resources)
23. [Validate ALPN and HTTP/2](#validate-alpn-and-http2)
24. [Test the Public gRPC Endpoint](#test-the-public-grpc-endpoint)
25. [API Governance and API Manager Positioning](#api-governance-and-api-manager-positioning)
26. [Troubleshooting Guide](#troubleshooting-guide)
27. [Production Hardening Checklist](#production-hardening-checklist)
28. [Acceptance Criteria](#acceptance-criteria)
29. [Cleanup](#cleanup)
30. [Suggested Demo Script Flow](#suggested-demo-script-flow)

---

# Lab Objective

The goal of this lab is to demonstrate that MuleSoft gRPC support is not just about consuming an external gRPC service.
<img width="1672" height="941" alt="image" src="https://github.com/user-attachments/assets/141b8202-1b41-428a-89e0-2cfbee3b0193" />

The target message is:

> MuleSoft now supports native gRPC across the API lifecycle: design, governance, Exchange publishing, APIkit scaffolding, Mule implementation, Runtime Fabric deployment, and API management positioning.

This lab focuses on a realistic backend-to-backend use case: **Order Tracking**.

You will demonstrate:

- Unary RPC with `GetOrderStatus`
- Server-streaming RPC with `StreamOrderEvents`
- Protobuf-first API design
- APIkit-generated Mule flows
- HTTP/2 listener configuration
- Runtime Fabric deployment on EKS
- HTTP/2 ingress with NGINX Ingress Controller
- ALPN validation at the TLS edge

---

# What You Will Build

You will build a gRPC service called:

```text
orders.v1.OrderTrackingService
```
<img width="1672" height="941" alt="image" src="https://github.com/user-attachments/assets/dee66bde-9779-48a4-a80b-3c3348e93863" />

It exposes two RPC methods:

```proto
service OrderTrackingService {
  rpc GetOrderStatus (OrderStatusRequest) returns (OrderStatusResponse);
  rpc StreamOrderEvents (OrderEventsRequest) returns (stream OrderEvent);
}
```

## Unary RPC

Unary RPC means one request and one response.

```text
Client request  →  Mule flow  →  Single response
```

Example request:

```json
{
  "order_id": "ORD-1042"
}
```

Example response:

```json
{
  "order_id": "ORD-1042",
  "status": "IN_TRANSIT",
  "estimated_delivery": "2026-06-24"
}
```

## Server-Streaming RPC

Server streaming means the client sends one request, and the server sends multiple response messages over the same connection.

```text
Client request  →  Mule flow  →  Event 1
                              →  Event 2
                              →  Event 3
```

Example response stream:

```json
{
  "event_type": "PICKED_UP",
  "event_time": "2026-06-23T09:00:00Z",
  "message": "Package collected from warehouse"
}
```

```json
{
  "event_type": "IN_TRANSIT",
  "event_time": "2026-06-23T11:30:00Z",
  "message": "Package is moving through the carrier network"
}
```

```json
{
  "event_type": "OUT_FOR_DELIVERY",
  "event_time": "2026-06-24T08:00:00Z",
  "message": "Package is out for delivery"
}
```

---

# Architecture

<img width="1672" height="941" alt="image" src="https://github.com/user-attachments/assets/fd8d3b12-54ac-42ad-ba0b-4253e304d41f" />

---

# Important Deployment Positioning

Do **not** frame this as a CloudHub 2.0 deployment tutorial.

For this native gRPC APIkit lab, the deployment target is:

```text
Runtime Fabric on Amazon EKS
```

The important positioning is:

```text
Runtime Fabric = Mule runtime hosting layer
NGINX Ingress = Kubernetes ingress layer
HTTP/2 = required transport for gRPC
API Governance = contract governance
API Manager / Gateway layer = API management and policy positioning
```

Use this phrasing in the video or lab:

> CloudHub 2.0 is not the focus of this lab. For native MuleSoft gRPC APIs, this demo uses Runtime Fabric because gRPC requires HTTP/2-capable ingress and Runtime Fabric is the documented deployment path for this use case.

Do not say:

```text
CloudHub 2.0 does not support HTTP/2.
```

That is too broad and not the right architectural statement.

Say this instead:

```text
CloudHub 2.0 is not currently positioned as the deployment target for this native gRPC APIkit lab. We are using Runtime Fabric on EKS with HTTP/2 ingress.
```

---

# gRPC, HTTP/2, and ALPN

## What is gRPC?

gRPC is a remote procedure call framework that commonly uses:

```text
Protobuf for contract and serialization
HTTP/2 for transport
Typed RPC methods instead of REST-style resources
```

## Why HTTP/2 Matters

gRPC depends on HTTP/2 features such as:

- Multiplexed streams
- Binary framing
- Header compression
- Long-lived streaming connections

A normal HTTP/1.1 ingress configuration is not enough for native gRPC.

## What is ALPN?

ALPN means:

```text
Application-Layer Protocol Negotiation
```

It is a TLS handshake feature where the client and server agree which application protocol to use before normal application traffic begins.

For this lab, the important protocol is:

```text
h2
```

`h2` means HTTP/2 over TLS.

The external connection should behave like this:

```text
grpcurl / client
  → “I support h2”

NGINX Ingress
  → “Use h2”

gRPC traffic starts over HTTP/2
```

If ALPN does not negotiate `h2` at the public TLS endpoint, the client may fall back to HTTP/1.1 or fail the gRPC connection.

For the video, say:

> ALPN is the TLS handshake step where the client and ingress agree to use HTTP/2, which gRPC requires.

---

# Prerequisites

## MuleSoft Prerequisites

You need:

- Anypoint Platform access
- Anypoint Code Builder access
- Permission to publish assets to Anypoint Exchange
- Permission to deploy applications to Runtime Fabric
- Runtime Fabric installed on Amazon EKS
- Mule runtime 4.11 or later
- HTTP Connector 1.11 or later
- APIkit for gRPC support

## Local Development Tools

Install:

```text
Java compatible with your selected Mule runtime
Maven
grpcurl
protoc
Git
kubectl
OpenSSL
```

Verify tools:

```bash
java -version
mvn -version
grpcurl --version
protoc --version
kubectl version --client
openssl version
```

## Kubernetes / EKS Prerequisites

You need:

- Amazon EKS cluster
- Runtime Fabric installed and active
- `kubectl` access to the cluster
- NGINX Ingress Controller installed
- A DNS zone or hostname for the gRPC endpoint
- TLS certificate for the hostname
- Access to the Runtime Fabric namespace, usually `rtf`

This lab assumes:

```text
Runtime Fabric namespace: rtf
Ingress controller namespace: ingress-nginx
Ingress class: nginx
Base domain: apps.muleaceacademy.com
gRPC hostname: orders.apps.muleaceacademy.com
TLS secret name: grpc-wildcard-tls
Mule app port: 8081
```

Replace these values for your own environment.

---

# Suggested Repository Structure

```text
mulesoft-grpc-order-tracking/
├── README.md
├── api/
│   └── order-tracking.proto
├── infrastructure/
│   └── rtf/
│       ├── grpc-nginx-route-template.yaml
│       └── grpc-ingress-tls-secret.yaml
├── mule-app/
│   ├── pom.xml
│   ├── mule-artifact.json
│   └── src/
│       └── main/
│           ├── mule/
│           │   └── order-tracking-grpc.xml
│           └── resources/
│               ├── config/
│               │   └── local.properties
│               └── grpc/
│                   └── order-tracking.protobin
└── scripts/
    ├── test-local-unary.sh
    ├── test-local-streaming.sh
    ├── test-rtf-unary.sh
    └── test-rtf-streaming.sh
```

The generated Mule project structure can differ depending on the APIkit for gRPC version and your Anypoint Code Builder workflow. Preserve the generated project structure and adapt the examples accordingly.

---

# Create the Protobuf Contract

Create this file:

```text
api/order-tracking.proto
```

```proto
syntax = "proto3";

package orders.v1;

service OrderTrackingService {
  rpc GetOrderStatus (OrderStatusRequest) returns (OrderStatusResponse);
  rpc StreamOrderEvents (OrderEventsRequest) returns (stream OrderEvent);
}

message OrderStatusRequest {
  string order_id = 1;
}

message OrderStatusResponse {
  string order_id = 1;
  string status = 2;
  string estimated_delivery = 3;
}

message OrderEventsRequest {
  string order_id = 1;
}

message OrderEvent {
  string event_type = 1;
  string event_time = 2;
  string message = 3;
}
```

## Contract Design Rules

Follow these rules:

- Use versioned package names such as `orders.v1`.
- Do not change existing field numbers.
- Do not reuse deleted field numbers.
- Add new fields instead of changing existing field types.
- Keep RPC method names stable.
- Treat the `.proto` file as a formal API contract.
- Publish the contract before scaffolding the Mule implementation.

---

# Design, Govern, and Publish in Anypoint Code Builder

In Anypoint Code Builder:

1. Create a new API specification.
2. Select **gRPC API**.
3. Select **Protobuf 3**.
4. Paste the `order-tracking.proto` contract.
5. Validate the contract.
6. Apply the gRPC governance ruleset if available in your organization.
7. Publish the API to Anypoint Exchange.

Suggested Exchange asset metadata:

```text
Name: order-tracking-grpc-api
Version: 1.0.0
API type: gRPC API
Specification type: Protobuf 3
Package: orders.v1
```

After publishing, the `.proto` contract becomes a discoverable and versioned API asset in Exchange.

---

# Scaffold the Mule Application with APIkit for gRPC

In Anypoint Code Builder:

1. Open the MuleSoft activity bar.
2. Select **Implement an API**.
3. Search Exchange for the gRPC API asset.
4. Select the `order-tracking-grpc-api` asset.
5. Choose Mule runtime **4.11 or later**.
6. Create the Mule project.

APIkit for gRPC generates:

- A Mule project
- A gRPC server configuration
- A flow for each RPC method
- A compiled `.protobin` descriptor
- gRPC connector dependencies in `pom.xml`
- Mule and Java settings in `mule-artifact.json`

Do not manually recreate the gRPC server configuration from scratch unless required.

The safest workflow is:

```text
Generate with APIkit → preserve generated config → add business logic inside generated flows
```

---

# Understand the Generated Mule Project

After scaffolding, inspect these files:

```text
src/main/mule/
  order-tracking-grpc.xml

src/main/resources/
  grpc/
    <generated-descriptor>.protobin
```

You should see a gRPC server configuration similar to:

```xml
<grpc:server-config name="GRPC_Server_Config">
  <grpc:grpc-server-connection listenerConfig="HTTP_Listener_Config">
    <grpc:idl-definition>
      <grpc:single-binary-protobuf protobufDescriptorFile="${grpc.server.descriptor.file}" />
    </grpc:idl-definition>
  </grpc:grpc-server-connection>
</grpc:server-config>
```

The gRPC server does not independently define host, port, or TLS. It uses a Mule HTTP Listener as the transport.

That means HTTP/2 must be configured in the HTTP Listener used by the gRPC server.

---

# Configure HTTP/2 in Mule

Create or update your properties file:

```text
src/main/resources/config/local.properties
```

```properties
grpc.server.host=0.0.0.0
grpc.server.port=8081

# Use the exact .protobin path generated by APIkit.
grpc.server.descriptor.file=grpc/order-tracking.protobin

grpc.stream.maxConcurrency=50
```

Find the generated `.protobin` file:

```bash
find src/main/resources -name "*.protobin"
```

Update `grpc.server.descriptor.file` to match the actual generated path.

## HTTP/2 Cleartext Listener for Local and In-Cluster gRPC

For this lab, TLS is terminated at NGINX Ingress. Traffic from NGINX to the Mule app is gRPC over HTTP/2 cleartext, also called H2C.

Use an HTTP Listener with HTTP/2 enabled:

```xml
<http:listener-config name="HTTP_Listener_Config">
    <http:listener-connection
        host="${grpc.server.host}"
        port="${grpc.server.port}"
        protocol="HTTP">

        <http:protocol-support>
            <http:http1-support enable="false" />
            <http:http2-support />
        </http:protocol-support>

    </http:listener-connection>
</http:listener-config>
```

This configuration means:

```text
Mule listener protocol: HTTP
HTTP/1.1: disabled
HTTP/2: enabled
TLS: not terminated inside Mule
```

That is appropriate when:

```text
Client → TLS + HTTP/2 → NGINX Ingress
NGINX → gRPC over H2C → Mule application
```

## NETTY Requirement

Configure Mule to use NETTY for HTTP/2.

For local runs, add this JVM property:

```text
-Dmule.http.service.implementation=NETTY
```

For Runtime Fabric deployment, add the same JVM property in Runtime Manager:

```text
-Dmule.http.service.implementation=NETTY
```

Even if your runtime version defaults to NETTY, setting it explicitly makes the lab easier to validate and troubleshoot.

---

# Implement the Unary RPC

APIkit creates a source for the unary RPC, similar to:

```xml
<grpc:unary-method
    config-ref="GRPC_Server_Config"
    methodName="orders.v1.OrderTrackingService/GetOrderStatus" />
```

Add business logic after the generated source.

Example implementation:

```xml
<flow name="get-order-status-flow">

    <grpc:unary-method
        config-ref="GRPC_Server_Config"
        methodName="orders.v1.OrderTrackingService/GetOrderStatus" />

    <choice>
        <when expression="#[isEmpty(payload.order_id default '')]">
            <raise-error
                type="GRPC:INVALID_ARGUMENT"
                description="order_id is required" />
        </when>
    </choice>

    <ee:transform doc:name="Build Order Status Response">
        <ee:message>
            <ee:set-payload><![CDATA[
%dw 2.0
output application/java
---
{
    order_id: payload.order_id,
    status: "IN_TRANSIT",
    estimated_delivery: "2026-06-24"
}
            ]]></ee:set-payload>
        </ee:message>
    </ee:transform>

</flow>
```

In a real implementation, this flow could call:

- Order Management System
- SAP
- Salesforce Commerce Cloud
- Warehouse Management System
- Carrier API
- Database
- Event store

The key point is that your response must match the Protobuf response message:

```proto
message OrderStatusResponse {
  string order_id = 1;
  string status = 2;
  string estimated_delivery = 3;
}
```

---

# Implement the Server-Streaming RPC

APIkit creates a source for the server-streaming RPC, similar to:

```xml
<grpc:server-streaming-method
    config-ref="GRPC_Server_Config"
    methodName="orders.v1.OrderTrackingService/StreamOrderEvents" />
```

For server streaming, the flow sends multiple messages using:

```xml
<grpc:send-stream-message config-ref="GRPC_Server_Config" />
```

Then it closes the stream using:

```xml
<grpc:end-stream config-ref="GRPC_Server_Config" />
```

Example flow:

```xml
<flow
    name="stream-order-events-flow"
    maxConcurrency="${grpc.stream.maxConcurrency}">

    <grpc:server-streaming-method
        config-ref="GRPC_Server_Config"
        methodName="orders.v1.OrderTrackingService/StreamOrderEvents"
        onCapacityOverload="DROP" />

    <choice>
        <when expression="#[isEmpty(payload.order_id default '')]">
            <raise-error
                type="GRPC:INVALID_ARGUMENT"
                description="order_id is required" />
        </when>
    </choice>

    <set-variable
        variableName="orderId"
        value="#[payload.order_id]" />

    <set-variable
        variableName="events"
        value="#[
            [
                {
                    event_type: 'PICKED_UP',
                    event_time: '2026-06-23T09:00:00Z',
                    message: 'Package collected from warehouse'
                },
                {
                    event_type: 'IN_TRANSIT',
                    event_time: '2026-06-23T11:30:00Z',
                    message: 'Package is moving through the carrier network'
                },
                {
                    event_type: 'OUT_FOR_DELIVERY',
                    event_time: '2026-06-24T08:00:00Z',
                    message: 'Package is out for delivery'
                }
            ]
        ]" />

    <foreach collection="#[vars.events]">

        <ee:transform doc:name="Build Order Event">
            <ee:message>
                <ee:set-payload><![CDATA[
%dw 2.0
output application/java
---
{
    event_type: payload.event_type,
    event_time: payload.event_time,
    message: payload.message
}
                ]]></ee:set-payload>
            </ee:message>
        </ee:transform>

        <grpc:send-stream-message
            config-ref="GRPC_Server_Config" />

    </foreach>

    <grpc:end-stream
        config-ref="GRPC_Server_Config" />

</flow>
```

## Streaming Notes

- Server streaming starts after the client sends a single request.
- Each call to `grpc:send-stream-message` pushes one response message.
- `grpc:end-stream` closes the stream.
- If the stream is not closed, the client can remain connected indefinitely.
- `maxConcurrency` controls how many stream requests the Mule flow can process concurrently.
- `onCapacityOverload="DROP"` rejects new streaming requests when capacity is full and returns a resource-exhausted style response.

---

# Build and Run Locally

From the Mule project directory:

```bash
cd mule-app
mvn clean package
```

Expected output:

```text
target/<application-name>-<version>-mule-application.jar
```

When running locally, make sure the runtime starts with:

```text
-Dmule.http.service.implementation=NETTY
```

The app should listen on:

```text
localhost:8081
```

---

# Test Locally with grpcurl

## Unary RPC

From the repository root:

```bash
grpcurl -plaintext \
  -import-path ./api \
  -proto order-tracking.proto \
  -d '{"order_id":"ORD-1042"}' \
  localhost:8081 \
  orders.v1.OrderTrackingService/GetOrderStatus
```

Expected response:

```json
{
  "order_id": "ORD-1042",
  "status": "IN_TRANSIT",
  "estimated_delivery": "2026-06-24"
}
```

## Server-Streaming RPC

```bash
grpcurl -plaintext \
  -import-path ./api \
  -proto order-tracking.proto \
  -d '{"order_id":"ORD-1042"}' \
  localhost:8081 \
  orders.v1.OrderTrackingService/StreamOrderEvents
```

Expected output:

```json
{
  "event_type": "PICKED_UP",
  "event_time": "2026-06-23T09:00:00Z",
  "message": "Package collected from warehouse"
}
{
  "event_type": "IN_TRANSIT",
  "event_time": "2026-06-23T11:30:00Z",
  "message": "Package is moving through the carrier network"
}
{
  "event_type": "OUT_FOR_DELIVERY",
  "event_time": "2026-06-24T08:00:00Z",
  "message": "Package is out for delivery"
}
```

## Negative Test

```bash
grpcurl -plaintext \
  -import-path ./api \
  -proto order-tracking.proto \
  -d '{}' \
  localhost:8081 \
  orders.v1.OrderTrackingService/GetOrderStatus
```

Expected result:

```text
Code: InvalidArgument
Message: order_id is required
```

---

# Prepare Runtime Fabric on EKS

Set environment variables for your lab:

```bash
export RTF_NAMESPACE=rtf
export INGRESS_NAMESPACE=ingress-nginx
export GRPC_BASE_DOMAIN=apps.muleaceacademy.com
export GRPC_HOST=orders.apps.muleaceacademy.com
export TLS_SECRET_NAME=grpc-wildcard-tls
```

Verify Runtime Fabric namespace:

```bash
kubectl get ns ${RTF_NAMESPACE}
```

Verify Runtime Fabric pods:

```bash
kubectl -n ${RTF_NAMESPACE} get pods
```

Verify NGINX Ingress Controller:

```bash
kubectl get ingressclass
kubectl -n ${INGRESS_NAMESPACE} get pods
kubectl -n ${INGRESS_NAMESPACE} get svc
```

Confirm your DNS record points to the external address of the NGINX Ingress Controller service.

```bash
kubectl -n ${INGRESS_NAMESPACE} get svc
```

Example DNS mapping:

```text
orders.apps.muleaceacademy.com → NGINX Ingress Controller LoadBalancer DNS name
```

---

# Configure HTTP/2 Ingress with NGINX

There are two layers to configure:

```text
Layer 1: Mule listener supports HTTP/2
Layer 2: Kubernetes ingress preserves gRPC / HTTP/2 traffic
```

The path should be:

```text
gRPC client
  ↓ TLS + HTTP/2
NGINX Ingress Controller
  ↓ gRPC / H2C
Mule HTTP/2 listener
```

## Check NGINX HTTP/2 Setting

Inspect the NGINX Ingress Controller ConfigMap:

```bash
kubectl -n ${INGRESS_NAMESPACE} get configmap ingress-nginx-controller -o yaml
```

Look for:

```yaml
data:
  use-http2: "true"
```

If it is missing or disabled, patch it:

```bash
kubectl -n ${INGRESS_NAMESPACE} patch configmap ingress-nginx-controller \
  --type merge \
  -p '{"data":{"use-http2":"true"}}'
```

Restart the controller:

```bash
kubectl -n ${INGRESS_NAMESPACE} rollout restart deployment ingress-nginx-controller
```

Validate rollout:

```bash
kubectl -n ${INGRESS_NAMESPACE} rollout status deployment ingress-nginx-controller
```

---

# Create and Synchronize the TLS Secret

Create a TLS secret in the Runtime Fabric namespace.

```bash
kubectl -n ${RTF_NAMESPACE} create secret tls ${TLS_SECRET_NAME} \
  --cert=./certs/tls.crt \
  --key=./certs/tls.key
```

Label it so Runtime Fabric can synchronize it into application namespaces:

```bash
kubectl -n ${RTF_NAMESPACE} label secret ${TLS_SECRET_NAME} \
  rtf.mulesoft.com/synchronized=true \
  --overwrite
```

Verify:

```bash
kubectl -n ${RTF_NAMESPACE} get secret ${TLS_SECRET_NAME} --show-labels
```

Expected label:

```text
rtf.mulesoft.com/synchronized=true
```

Your certificate must include the public gRPC hostname.

For this lab:

```text
orders.apps.muleaceacademy.com
```

A wildcard certificate also works:

```text
*.apps.muleaceacademy.com
```

---

# Create the Runtime Fabric HTTPRouteTemplate

Create this file:

```text
infrastructure/rtf/grpc-nginx-route-template.yaml
```

```yaml
apiVersion: rtf.mulesoft.com/v1
kind: HTTPRouteTemplate
metadata:
  name: grpc-nginx-route
  namespace: rtf
spec:
  baseEndpoints:
    - https://*.apps.muleaceacademy.com

  resources:
    - |
      apiVersion: networking.k8s.io/v1
      kind: Ingress
      metadata:
        name: {{ .ResourceName }}
        namespace: {{ .Namespace }}
        annotations:
          nginx.ingress.kubernetes.io/ssl-redirect: "true"
          nginx.ingress.kubernetes.io/backend-protocol: "GRPC"
          nginx.ingress.kubernetes.io/proxy-connect-timeout: "15"
          nginx.ingress.kubernetes.io/proxy-read-timeout: "3600"
          nginx.ingress.kubernetes.io/proxy-send-timeout: "3600"
      spec:
        ingressClassName: nginx
        tls:
          - hosts:
              - {{ .Host }}
            secretName: grpc-wildcard-tls
        rules:
          - host: {{ .Host }}
            http:
              paths:
                - path: {{ .Path }}
                  pathType: Prefix
                  backend:
                    service:
                      name: {{ .Service.Name }}
                      port:
                        name: {{ .Service.PortName }}
```

Apply it:

```bash
kubectl apply -f infrastructure/rtf/grpc-nginx-route-template.yaml
```

Verify:

```bash
kubectl -n rtf get httproutetemplate
kubectl -n rtf describe httproutetemplate grpc-nginx-route
```

## Critical NGINX Annotation

This is the most important annotation for the lab:

```yaml
nginx.ingress.kubernetes.io/backend-protocol: "GRPC"
```

It tells NGINX to proxy traffic to the backend using gRPC / HTTP/2 semantics.

Use:

```text
GRPC
```

when TLS terminates at NGINX and traffic from NGINX to Mule is cleartext HTTP/2.

Use:

```text
GRPCS
```

only when the backend Mule listener itself terminates TLS.

This lab uses `GRPC`, not `GRPCS`.

## Important Template Behavior

HTTPRouteTemplates apply to new Mule app deployments created after the template exists.

If you deploy the Mule app before creating the template, redeploy the Mule app after creating the template.

If you modify the template later, existing generated ingress resources are not automatically updated. Redeploy the Mule app to regenerate the route.

---

# Deploy the Mule App to Runtime Fabric

In Runtime Manager:

1. Go to **Applications**.
2. Click **Deploy application**.
3. Select the target Runtime Fabric.
4. Upload the Mule application JAR.
5. Select Mule runtime **4.11 or later**.
6. Configure replicas and resources based on your lab environment.
7. Open the **Ingress** section.
8. Select the base endpoint:

```text
https://*.apps.muleaceacademy.com
```

9. Set subdomain:

```text
orders
```

10. Set path:

```text
/
```

Expected public endpoint:

```text
https://orders.apps.muleaceacademy.com
```

11. Add JVM argument:

```text
-Dmule.http.service.implementation=NETTY
```

12. Add application properties:

```properties
grpc.server.host=0.0.0.0
grpc.server.port=8081
grpc.server.descriptor.file=grpc/order-tracking.protobin
grpc.stream.maxConcurrency=50
```

13. Deploy the application.

---

# Verify Kubernetes Resources

Find the application namespace:

```bash
kubectl get namespaces --show-labels | grep -i rtf
```

Or list recent namespaces:

```bash
kubectl get ns --sort-by=.metadata.creationTimestamp
```

Set the app namespace:

```bash
export APP_NAMESPACE=<application-namespace>
```

Verify pods:

```bash
kubectl -n ${APP_NAMESPACE} get pods
```

Verify services:

```bash
kubectl -n ${APP_NAMESPACE} get svc
```

Verify ingress:

```bash
kubectl -n ${APP_NAMESPACE} get ingress
```

Inspect the generated ingress:

```bash
kubectl -n ${APP_NAMESPACE} get ingress -o yaml
```

Confirm the generated ingress contains:

```yaml
spec:
  ingressClassName: nginx
```

```yaml
annotations:
  nginx.ingress.kubernetes.io/backend-protocol: GRPC
```

```yaml
tls:
  - hosts:
      - orders.apps.muleaceacademy.com
    secretName: grpc-wildcard-tls
```

```yaml
paths:
  - path: /
    pathType: Prefix
```

---

# Validate ALPN and HTTP/2

ALPN validates whether the public TLS endpoint negotiates HTTP/2.

Run:

```bash
openssl s_client \
  -connect orders.apps.muleaceacademy.com:443 \
  -servername orders.apps.muleaceacademy.com \
  -alpn h2 < /dev/null 2>/dev/null | grep ALPN
```

Expected result:

```text
ALPN protocol: h2
```

If you do not see `h2`, the public TLS endpoint is not negotiating HTTP/2 correctly.

That usually means one of these is wrong:

- NGINX HTTP/2 is disabled.
- TLS is terminating at the wrong layer.
- The wrong ingress controller is handling the hostname.
- DNS points to the wrong load balancer.
- The client is not connecting to the intended endpoint.

ALPN validation proves the public edge supports HTTP/2, but it does not prove the backend Mule route works. Always run `grpcurl` after the ALPN test.

---

# Test the Public gRPC Endpoint

## Unary RPC Through Runtime Fabric

```bash
grpcurl -vv \
  -import-path ./api \
  -proto order-tracking.proto \
  -d '{"order_id":"ORD-1042"}' \
  orders.apps.muleaceacademy.com:443 \
  orders.v1.OrderTrackingService/GetOrderStatus
```

Expected response:

```json
{
  "order_id": "ORD-1042",
  "status": "IN_TRANSIT",
  "estimated_delivery": "2026-06-24"
}
```

## Server-Streaming RPC Through Runtime Fabric

```bash
grpcurl -vv \
  -import-path ./api \
  -proto order-tracking.proto \
  -d '{"order_id":"ORD-1042"}' \
  orders.apps.muleaceacademy.com:443 \
  orders.v1.OrderTrackingService/StreamOrderEvents
```

Expected response stream:

```json
{
  "event_type": "PICKED_UP",
  "event_time": "2026-06-23T09:00:00Z",
  "message": "Package collected from warehouse"
}
{
  "event_type": "IN_TRANSIT",
  "event_time": "2026-06-23T11:30:00Z",
  "message": "Package is moving through the carrier network"
}
{
  "event_type": "OUT_FOR_DELIVERY",
  "event_time": "2026-06-24T08:00:00Z",
  "message": "Package is out for delivery"
}
```

## Self-Signed Certificate Option

For a non-production lab with a self-signed certificate:

```bash
grpcurl -insecure \
  -import-path ./api \
  -proto order-tracking.proto \
  -d '{"order_id":"ORD-1042"}' \
  orders.apps.muleaceacademy.com:443 \
  orders.v1.OrderTrackingService/GetOrderStatus
```

Do not use `-insecure` in production.

---

# API Governance and API Manager Positioning

Separate these concerns clearly:

```text
Runtime Fabric
  Runs the Mule gRPC implementation.

API Governance
  Validates and governs the Protobuf contract.

Exchange
  Publishes and discovers the gRPC API asset.

APIkit for gRPC
  Scaffolds the Mule implementation from the Protobuf contract.

API Manager / gateway layer
  Manages API instances, policies, and lifecycle positioning.
```

For this Runtime Fabric lab, do not imply that direct Runtime Fabric ingress automatically gives the same policy-enforcement behavior as a gateway-managed gRPC API path.

Use this wording:

> Runtime Fabric runs the Mule gRPC service. API Governance manages the contract quality. For centralized policy enforcement and API gateway behavior, position the appropriate gateway layer separately.

---

# Troubleshooting Guide

## Problem: `ALPN protocol: h2` is missing

Symptoms:

```text
openssl output does not show ALPN protocol: h2
grpcurl fails before reaching Mule
Client appears to fall back to HTTP/1.1
```

Check:

```bash
kubectl -n ingress-nginx get configmap ingress-nginx-controller -o yaml
```

Confirm:

```yaml
use-http2: "true"
```

Check DNS:

```bash
nslookup orders.apps.muleaceacademy.com
```

Check ingress controller service:

```bash
kubectl -n ingress-nginx get svc
```

Validate that DNS points to the right load balancer.

---

## Problem: `UNAVAILABLE`

Common causes:

- Mule pod is not ready.
- Kubernetes service has no endpoints.
- Ingress points to the wrong service.
- NGINX is not using `backend-protocol: GRPC`.
- Mule listener does not support HTTP/2.
- NETTY JVM property is missing.
- Wrong port configured in Mule or Runtime Fabric.

Commands:

```bash
kubectl -n ${APP_NAMESPACE} get pods
kubectl -n ${APP_NAMESPACE} get svc
kubectl -n ${APP_NAMESPACE} get endpoints
kubectl -n ${APP_NAMESPACE} get ingress -o yaml
```

Check Mule logs:

```bash
kubectl -n ${APP_NAMESPACE} logs <mule-pod-name> --all-containers=true --tail=200
```

Check NGINX logs:

```bash
kubectl -n ingress-nginx logs deployment/ingress-nginx-controller --tail=200
```

---

## Problem: `UNIMPLEMENTED`

Common causes:

- Wrong package name
- Wrong service name
- Wrong method name
- Client is using an outdated `.proto`
- Mule is using an outdated `.protobin`
- APIkit generated method name differs from the client call

Verify method names:

```text
orders.v1.OrderTrackingService/GetOrderStatus
orders.v1.OrderTrackingService/StreamOrderEvents
```

Verify Protobuf package:

```proto
package orders.v1;
```

Verify generated `.protobin` path:

```bash
find src/main/resources -name "*.protobin"
```

---

## Problem: `unexpected content-type` or HTTP/1.1 errors

Common causes:

- Ingress is proxying as normal HTTP instead of gRPC.
- `backend-protocol: GRPC` is missing.
- Mule listener is not HTTP/2-enabled.
- Client is not using gRPC.
- TLS termination is happening in the wrong place.

Confirm ingress annotation:

```yaml
nginx.ingress.kubernetes.io/backend-protocol: "GRPC"
```

Confirm Mule listener:

```xml
<http:protocol-support>
    <http:http1-support enable="false" />
    <http:http2-support />
</http:protocol-support>
```

Confirm JVM property:

```text
-Dmule.http.service.implementation=NETTY
```

---

## Problem: Unary works, streaming fails

This is common when the basic request path works but long-lived streams are not configured correctly.

Check NGINX timeouts:

```yaml
nginx.ingress.kubernetes.io/proxy-read-timeout: "3600"
nginx.ingress.kubernetes.io/proxy-send-timeout: "3600"
```

Check AWS load balancer idle timeout.

Check whether the Mule flow calls:

```xml
<grpc:end-stream config-ref="GRPC_Server_Config" />
```

Check whether the client has a deadline that is too short.

---

## Problem: `RESOURCE_EXHAUSTED`

Possible cause:

```text
The server-streaming flow reached maxConcurrency and onCapacityOverload="DROP" rejected the request.
```

Current lab setting:

```properties
grpc.stream.maxConcurrency=50
```

Possible fixes:

- Increase `grpc.stream.maxConcurrency`.
- Increase Runtime Fabric replicas.
- Reduce stream duration.
- Avoid long-lived idle streams.
- Use client retry and reconnect logic.
- Use event filtering to reduce stream volume.

---

## Problem: Descriptor file not found

Find the descriptor:

```bash
find src/main/resources -name "*.protobin"
```

Update property:

```properties
grpc.server.descriptor.file=grpc/<actual-file-name>.protobin
```

Do not guess the descriptor path.

Use the file and path generated by APIkit.

---

## Problem: TLS secret not found in app namespace

Verify the source secret:

```bash
kubectl -n rtf get secret grpc-wildcard-tls
```

Verify label:

```bash
kubectl -n rtf get secret grpc-wildcard-tls --show-labels
```

Expected:

```text
rtf.mulesoft.com/synchronized=true
```

Verify the app namespace:

```bash
kubectl -n ${APP_NAMESPACE} get secret grpc-wildcard-tls
```

If it is missing, confirm Runtime Fabric secret synchronization and redeploy the app if needed.

---

## Problem: Runtime Manager does not show the expected ingress endpoint

Common causes:

- HTTPRouteTemplate was not created.
- HTTPRouteTemplate was created in the wrong namespace.
- Runtime Fabric agent did not sync the template.
- The app was deployed before the template existed.
- Template YAML has an error.
- Base endpoint does not match expected host pattern.

Check:

```bash
kubectl -n rtf get httproutetemplate
kubectl -n rtf describe httproutetemplate grpc-nginx-route
```

Redeploy the Mule app after fixing the template.

---

# Production Hardening Checklist

## Security

- Use trusted TLS certificates.
- Avoid self-signed certificates in production.
- Use a formal certificate rotation process.
- Use secure secret management.
- Use encrypted Mule properties where needed.
- Add authentication and authorization.
- Consider mTLS for service-to-service traffic.
- Restrict ingress by source IP or network policy where appropriate.
- Validate which gateway layer enforces API policies.

## API Lifecycle

- Publish the Protobuf contract to Exchange.
- Apply API Governance rules before publishing.
- Document RPC methods clearly.
- Document gRPC status codes.
- Document deadlines and retry behavior.
- Version packages intentionally, such as `orders.v1`.
- Never reuse Protobuf field numbers.
- Maintain backward compatibility.

## Streaming

- Define stream duration limits.
- Define idle timeout behavior.
- Handle client cancellation.
- Handle reconnects.
- Avoid unbounded streams.
- Consider heartbeat messages for long-lived streams.
- Tune NGINX and AWS load balancer idle timeouts.
- Monitor concurrent streams separately from request rate.

## Performance

- Load test unary and streaming RPCs separately.
- Monitor CPU, heap, and GC.
- Monitor concurrent streams.
- Tune `maxConcurrency`.
- Scale Runtime Fabric replicas.
- Validate behavior during pod restarts.
- Validate behavior during rolling deployments.

## Observability

- Add correlation IDs.
- Add structured logging.
- Track gRPC status codes.
- Monitor error rates.
- Monitor ingress latency.
- Monitor stream durations.
- Monitor dropped or rejected stream requests.

---

# Acceptance Criteria

The lab is complete when all items pass:

```text
[ ] Protobuf contract is created
[ ] gRPC API is published to Anypoint Exchange
[ ] Mule project is scaffolded with APIkit for gRPC
[ ] Generated .protobin descriptor exists
[ ] Mule runtime is 4.11 or later
[ ] HTTP Connector is 1.11 or later
[ ] Mule HTTP Listener has HTTP/2 enabled
[ ] NETTY JVM property is configured
[ ] Unary RPC works locally
[ ] Server-streaming RPC works locally
[ ] Runtime Fabric app deploys successfully
[ ] HTTPRouteTemplate exists in the rtf namespace
[ ] TLS secret is synchronized
[ ] Generated ingress has backend-protocol: GRPC
[ ] Public endpoint negotiates ALPN h2
[ ] Unary RPC works through Runtime Fabric endpoint
[ ] Server-streaming RPC works through Runtime Fabric endpoint
[ ] Troubleshooting commands are documented
[ ] Cleanup steps are tested
```

---

# Cleanup

Delete the Mule application from Runtime Manager.

Delete the HTTPRouteTemplate only if no other apps depend on it:

```bash
kubectl -n rtf delete httproutetemplate grpc-nginx-route
```

Delete the TLS secret only if it was created only for this lab:

```bash
kubectl -n rtf delete secret grpc-wildcard-tls
```

Remove local build artifacts:

```bash
cd mule-app
mvn clean
```

Unset environment variables:

```bash
unset RTF_NAMESPACE
unset INGRESS_NAMESPACE
unset GRPC_BASE_DOMAIN
unset GRPC_HOST
unset TLS_SECRET_NAME
unset APP_NAMESPACE
```

---

# Suggested Demo Script Flow

Use this flow for recording the video:

1. Explain why REST polling is not ideal for live order tracking.
2. Introduce gRPC and Protobuf.
3. Show the `.proto` contract.
4. Explain unary vs server streaming.
5. Publish the gRPC API to Exchange.
6. Scaffold the Mule app with APIkit for gRPC.
7. Show generated flows and `.protobin` descriptor.
8. Configure HTTP/2 in the Mule HTTP Listener.
9. Implement `GetOrderStatus`.
10. Implement `StreamOrderEvents`.
11. Test locally with `grpcurl`.
12. Explain Runtime Fabric and HTTP/2 ingress.
13. Create the NGINX HTTPRouteTemplate.
14. Deploy to Runtime Fabric on EKS.
15. Validate ALPN with OpenSSL.
16. Test unary RPC through the public endpoint.
17. Test server streaming through the public endpoint.
18. Explain Runtime Fabric vs gateway/API management positioning.
19. End with production hardening and next steps.

---

# Key Phrases for the Video

Use these lines during recording:

```text
This is not just a gRPC connector demo. This is gRPC across the MuleSoft API lifecycle.
```

```text
APIkit creates a Mule flow for each RPC method defined in the Protobuf contract.
```

```text
The .protobin descriptor is the compiled contract the Mule gRPC server uses at runtime.
```

```text
For Runtime Fabric on EKS, enabling HTTP/2 is not one checkbox. Mule must support HTTP/2, and the Kubernetes ingress must preserve gRPC traffic.
```

```text
ALPN is the TLS handshake step where the client and ingress agree to use HTTP/2.
```

```text
Unary working does not prove streaming is configured correctly.
```

```text
Runtime Fabric runs the Mule gRPC service. The gateway layer is where you position centralized policy enforcement.
```

---

# Next Enhancements

After completing this lab, extend it with:

1. Client-streaming RPC for batch order updates.
2. Bidirectional streaming for live driver or delivery telemetry.
3. Kafka-backed order event streaming.
4. OAuth or mTLS security.
5. MUnit tests for unary and streaming flows.
6. API Governance rules for Protobuf naming and versioning.
7. Observability with distributed tracing.
8. Flex Gateway or another gateway layer for centralized policy enforcement.
9. Load testing for concurrent streams.
10. A real backend integration with SAP, OMS, or carrier tracking APIs.
