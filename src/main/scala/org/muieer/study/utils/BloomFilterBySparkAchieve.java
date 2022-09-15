package org.muieer.study.utils;

import org.apache.spark.util.sketch.BloomFilter;

public class BloomFilterBySparkAchieve {

    public BloomFilter create(long num) {

        return BloomFilter.create(num);
    }

    public static void demoBySparkImpl() {

        BloomFilter bloomFilter = BloomFilter.create(3);

        bloomFilter.putString("a");
        bloomFilter.putString("b");
        bloomFilter.putString("c");
        bloomFilter.putString("d");
        bloomFilter.putLong(1);

        System.out.println(bloomFilter.mightContainString("e"));
        System.out.println(bloomFilter.mightContainString("a"));
        System.out.println(bloomFilter.mightContainLong(1));

        System.out.println(Math.log(Math.E)); // 底数是自然数e
        System.out.println(1L<<63);

    }

}
