import javax.servlet.*;
import java.io.IOException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ResourceBundle;

import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class HelloFilter implements Filter {
    @Override
    public void init(FilterConfig arg0) throws ServletException {
        System.out.println("Filter 初始化");
    }
    @Override
    public void doFilter(ServletRequest req, ServletResponse res,
                         FilterChain chain) throws IOException, ServletException {
        HttpServletRequest request = (HttpServletRequest)req;
        System.out.println("拦截 URI="+request.getRequestURI());
        chain.doFilter(req, res);
    }
    @Override
    public void destroy() {
        System.out.println("Filter 结束");
    }
}