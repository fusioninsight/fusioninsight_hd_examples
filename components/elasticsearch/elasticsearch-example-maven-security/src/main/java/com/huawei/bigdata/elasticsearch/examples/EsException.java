package com.huawei.bigdata.elasticsearch.examples;

public class EsException extends Exception
{
    static final long serialVersionUID = 0;

    public EsException()
    {
        super();
    }

    public EsException(String message)
    {
        super(message);
    }

    public EsException(Throwable cause)
    {
        super(cause);
    }

    public EsException(String message, Throwable cause)
    {
        super(message, cause);
    }

}
