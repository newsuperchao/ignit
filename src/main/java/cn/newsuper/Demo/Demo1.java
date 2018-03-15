package cn.newsuper.Demo;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;

import java.util.Arrays;
import java.util.Scanner;

/**
 * Created by Administrator on 2018/3/15.
 */
public class Demo1 {

    private static Integer test_rows =5000;
    private static Integer thread_num=200;
    private static Ignite ignite;
    public static void main(String[] args) {
        keyMap();
        ignite = getIgnite();
       // testWrite();
        testRead();
        System.out.println("线程数"+thread_num+"数据量："+test_rows);
        ignite.close();
    }

    /***
     * 键盘设置参数
     * @return
     */
    private static void keyMap(){
        //测试数据行数
        System.out.println("测试数据行数");
        Scanner sc1 = new Scanner(System.in);
        test_rows = sc1.nextInt();
        //测试线程数
        // System.out.println("测试线程数：");
        //   Scanner sc2 = new Scanner(System.in);
        // thread_num= sc2.nextInt();
    }
    private static void testWrite(){
        Thread[] threads = new Thread[thread_num];


        final long start = System.currentTimeMillis();
        IgniteCache<String,String> cache = ignite.getOrCreateCache("Ignite_test");
        for (int i =0;i<threads.length;i++){
            threads[i] = new Thread(new IgniteThread(false, cache));
        }
        for (int i = 0; i<threads.length;i++){
            threads[i].start();
        }
        for (Thread thread:threads){
            try {
                thread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        final long end = System.currentTimeMillis();
        float times = end-start;
        float count = (float)test_rows/times;
        System.out.println("插入数据花费时间："+(end-start)+"ms");
        System.out.println("每秒插入数据量："+count*1000);

    }

    private static  void testRead(){
        Thread[] read_Thread = new Thread[thread_num];
        final long start = System.currentTimeMillis();
        IgniteCache<String,String> cache = ignite.getOrCreateCache("Ignite_test");
        for (int i =0;i<read_Thread.length;i++){
            read_Thread[i] = new Thread(new IgniteThread(true, cache));
        }
        for (int i = 0; i<read_Thread.length;i++){
            read_Thread[i].start();
        }
        for (Thread thread:read_Thread){
            try {
                thread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        final long end = System.currentTimeMillis();
        float times = end-start;
        float count = (float)test_rows/times;
        System.out.println("读取数据花费时间："+(end-start)+"ms");
        System.out.println("每秒读取数据量："+count*1000);
    }
    private static Ignite getIgnite()
    {
        TcpDiscoverySpi spi = new TcpDiscoverySpi();
        TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder();
        ipFinder.setAddresses(Arrays.asList("192.168.1.172:47500..47509"));
        spi.setIpFinder(ipFinder);
        // CacheConfiguration cacheConfiguration = new CacheConfiguration<String,String>();
        //cacheConfiguration.setCacheMode(CacheMode.LOCAL);
        //  cacheConfiguration.setBackups(1);
        IgniteConfiguration cfg = new IgniteConfiguration();

        cfg.setDiscoverySpi(spi);
        cfg.setClientMode(true);

        ignite = Ignition.start(cfg);
        return ignite;
    }
    static class IgniteThread implements Runnable {
        private static boolean startFlag = true;
        private static IgniteCache<String, String> cache;

        IgniteThread(boolean startFlag, IgniteCache<String, String> cache) {
            this.startFlag = startFlag;
            this.cache = cache;
        }

        public void run() {
            for (int i = 0; i < test_rows / thread_num;i++ ) {
                if (this.startFlag) {
                    cache.get(Integer.toString(i));
                } else {
                    cache.put(Integer.toString(i), "aa" + i);
                }
            }
        }
    }
}

