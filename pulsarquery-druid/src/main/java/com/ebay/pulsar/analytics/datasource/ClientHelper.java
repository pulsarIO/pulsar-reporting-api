/*******************************************************************************
*  Copyright © 2012-2015 eBay Software Foundation
*  This program is dual licensed under the MIT and Apache 2.0 licenses.
*  Please see LICENSE for more information.
*******************************************************************************/

package com.ebay.pulsar.analytics.datasource;

import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import javax.ws.rs.client.Client;

import org.jboss.resteasy.client.jaxrs.ResteasyClientBuilder;

/**
 * 
 * @author rtao
 *
 */
public class ClientHelper {
	
    public static Client initSSLClient() throws NoSuchAlgorithmException, KeyManagementException {
        SSLContext ctx = SSLContext.getInstance("SSL");
        ctx.init(null, certs, new SecureRandom());
        return new ResteasyClientBuilder() 
                .connectionPoolSize(20)
                .connectionTTL(2, TimeUnit.SECONDS)
                .establishConnectionTimeout(10, TimeUnit.SECONDS)
                .socketTimeout(60, TimeUnit.SECONDS)
                .hostnameVerifier(new TrustAllHostNameVerifier())
                .sslContext(ctx)
                .build();
    }
    
    public static Client initClient() {
        return new ResteasyClientBuilder()
                 .connectionPoolSize(20)
                 .connectionTTL(2, TimeUnit.SECONDS)
                 .establishConnectionTimeout(10, TimeUnit.SECONDS)
                 .socketTimeout(60, TimeUnit.SECONDS)
                 .build();
    }

    static TrustManager[] certs = new TrustManager[]{
            new X509TrustManager() {
                @Override
                public X509Certificate[] getAcceptedIssuers() {
                    return null;
                }

                @Override
                public void checkServerTrusted(X509Certificate[] chain, String authType)
                        throws CertificateException {
                }

                @Override
                public void checkClientTrusted(X509Certificate[] chain, String authType)
                        throws CertificateException {
                }
            }
    };

    public static class TrustAllHostNameVerifier implements HostnameVerifier {

        public boolean verify(String hostname, SSLSession session) {
            return true;
        }

    }
}
