package org.apache.hadoop.jmx;

import org.apache.hadoop.metrics2.util.MBeans;

import javax.management.*;
import java.lang.management.ManagementFactory;

public class TestJMX {
    public static void main(String[] args)throws Exception {

        TestJMX testJMX=new TestJMX();
        String serviceName="DataNode";
        String nameName="DataNodeInfo";
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        ObjectName name = MBeans.getMBeanName(serviceName, nameName);

        mbs.registerMBean(Dy.class, name);
        System.out.println("启动成功");
    }
    public class Dy implements DynamicMBean {
        @Override
        public Object getAttribute(String attribute) throws AttributeNotFoundException, MBeanException, ReflectionException {
            return null;
        }

        @Override
        public void setAttribute(Attribute attribute) throws AttributeNotFoundException, InvalidAttributeValueException, MBeanException, ReflectionException {

        }

        @Override
        public AttributeList getAttributes(String[] attributes) {
            return null;
        }

        @Override
        public AttributeList setAttributes(AttributeList attributes) {
            return null;
        }

        @Override
        public Object invoke(String actionName, Object[] params, String[] signature) throws MBeanException, ReflectionException {
            return null;
        }

        @Override
        public MBeanInfo getMBeanInfo() {
            return null;
        }
    }
}
