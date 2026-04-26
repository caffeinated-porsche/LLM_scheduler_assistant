#!/bin/bash
set -e

# Setup variables
NAMESPACE="default"
SERVICE="llm-webhook-service"
SECRET="llm-webhook-tls"

echo "Generating TLS certificates..."
# Use a local directory instead of /tmp to avoid Windows path issues
tmpdir="./certs_tmp"
mkdir -p ${tmpdir}

# 1. Generate the CA (Notice the double slash //CN which stops Git Bash path mangling)
openssl req -nodes -new -x509 -keyout ${tmpdir}/ca.key -out ${tmpdir}/ca.crt -subj "//CN=Admission Controller Webhook CA"

# 2. Generate the server certificate signing request (CSR)
openssl req -nodes -new -keyout ${tmpdir}/tls.key -out ${tmpdir}/tls.csr -subj "//CN=${SERVICE}.${NAMESPACE}.svc"

# 3. Create an extensions config file for the cert
cat <<EOF > ${tmpdir}/extfile.conf
authorityKeyIdentifier=keyid,issuer
basicConstraints=CA:FALSE
subjectAltName = @alt_names
[alt_names]
DNS.1 = ${SERVICE}
DNS.2 = ${SERVICE}.${NAMESPACE}
DNS.3 = ${SERVICE}.${NAMESPACE}.svc
EOF

# 4. Sign the server certificate
openssl x509 -req -in ${tmpdir}/tls.csr -CA ${tmpdir}/ca.crt -CAkey ${tmpdir}/ca.key -CAcreateserial -out ${tmpdir}/tls.crt -days 365 -extfile ${tmpdir}/extfile.conf

# 5. Create a Kubernetes Secret containing the certs
kubectl create secret tls ${SECRET} \
    --cert=${tmpdir}/tls.crt \
    --key=${tmpdir}/tls.key \
    --namespace=${NAMESPACE} \
    --dry-run=client -o yaml | kubectl apply -f -

# 6. Extract the CA bundle (Base64) to inject into the MutatingWebhookConfiguration
CA_BUNDLE=$(cat ${tmpdir}/ca.crt | base64 | tr -d '\n')

echo "Injecting CA bundle into webhook-manifest.yaml..."
sed -e "s|\${CA_BUNDLE}|${CA_BUNDLE}|g" webhook-manifest-template.yaml > webhook-manifest.yaml

echo "Done! Certificates created and webhook-manifest.yaml generated."
# Clean up the local temp directory
rm -rf ${tmpdir}