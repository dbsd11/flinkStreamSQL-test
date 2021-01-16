package com.gotin.flink.sql.source.avatica;

import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.remote.AvaticaCommonsHttpClientImpl;
import org.apache.calcite.avatica.remote.Service;
import org.apache.flink.calcite.shaded.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URL;

public class MyAvaticaHttpClient extends AvaticaCommonsHttpClientImpl {

    //这个ObjectMapper比较特殊，必须得使用flink集成的
    private ObjectMapper avatiaObjectMapper = new ObjectMapper();

    public MyAvaticaHttpClient(URL url) {
        super(url);
    }

    @Override
    public byte[] send(byte[] request) {
        byte[] response = super.send(request);
        try {
            Service.Response resp = avatiaObjectMapper.readValue(response, Service.Response.class);
            if ((resp instanceof Service.ExecuteResponse) && ((Service.ExecuteResponse) resp).missingStatement) {
                Service.ExecuteRequest executeRequest = avatiaObjectMapper.readValue(request, Service.ExecuteRequest.class);
                Service.PrepareResponse prepareResponse = prepare(new Service.PrepareRequest(executeRequest.statementHandle.connectionId, executeRequest.statementHandle.signature.sql, -1));

                Field statementIdField = Meta.StatementHandle.class.getDeclaredField("id");
                statementIdField.setAccessible(true);
                statementIdField.setInt(executeRequest.statementHandle, prepareResponse.statement.id);

                byte[] newStatementRequest = avatiaObjectMapper.writeValueAsBytes(executeRequest);
                response = super.send(newStatementRequest);
            }

        } catch (Exception e) {
            System.out.println("MyAvaticaHttpClient send optimize failed, return origin response");
        }

        return response;
    }

    Service.PrepareResponse prepare(Service.PrepareRequest prepareRequest) throws IOException {
        byte[] sendResult = this.send(avatiaObjectMapper.writeValueAsBytes(prepareRequest));
        Service.Response resp = avatiaObjectMapper.readValue(sendResult, Service.Response.class);
        if (resp instanceof Service.ErrorResponse) {
            throw ((Service.ErrorResponse) resp).toException();
        } else if (!Service.PrepareResponse.class.isAssignableFrom(resp.getClass())) {
            throw new ClassCastException("Cannot cast " + resp.getClass() + " into Service.PrepareResponse");
        } else {
            return Service.PrepareResponse.class.cast(resp);
        }
    }
}
