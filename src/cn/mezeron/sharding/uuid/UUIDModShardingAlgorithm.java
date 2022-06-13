package cn.mezeron.sharding.uuid;

import com.google.common.base.Preconditions;
import org.apache.shardingsphere.sharding.api.sharding.ShardingAutoTableAlgorithm;
import org.apache.shardingsphere.sharding.api.sharding.standard.PreciseShardingValue;
import org.apache.shardingsphere.sharding.api.sharding.standard.RangeShardingValue;
import org.apache.shardingsphere.sharding.api.sharding.standard.StandardShardingAlgorithm;

import java.util.*;

/**
 * 自定义分片规则
 * @Author: happyi
 * @Date: 2021/11/19 上午10:19
 */
public class UUIDModShardingAlgorithm implements  StandardShardingAlgorithm<String>, ShardingAutoTableAlgorithm {

    private static final String SHARDING_COUNT_KEY = "sharding-count";
    private static final int ASCII_NUMBER_MIN =48;
    private static final int ASCII_NUMBER_MAX =57;
    private static final int ASCII_LETTER_MIN =97;
    private static final int ASCII_LETTER_MAX =122;
    private Properties props = new Properties();

    private int shardingCount;

    private int getShardingCount() {
        Preconditions.checkArgument(props.containsKey(SHARDING_COUNT_KEY), "sharding-count cannot be null.");
        return Integer.parseInt(props.getProperty(SHARDING_COUNT_KEY));
    }

    @Override
    public int getAutoTablesAmount() {
        return shardingCount;
    }

    @Override
    public Collection<String> getAllPropertyKeys() {
        return Collections.singletonList(SHARDING_COUNT_KEY);
    }

    @Override
    public String doSharding(Collection<String> availableTargetNames, PreciseShardingValue<String> shardingValue) {
        for (String each : availableTargetNames) {
            if (each.endsWith(String.valueOf(shardingValue(shardingValue.getValue()) % shardingCount))) {
                return each;
            }
        }
        return null;
    }
    private long shardingValue(String shardingValue) {
        char last = shardingValue.charAt(shardingValue.length()-1);
        int value = last;
        if(value>= ASCII_NUMBER_MIN && value <= ASCII_NUMBER_MAX){
            value = value-ASCII_NUMBER_MIN;
        }else if(value>=ASCII_LETTER_MIN && value<=ASCII_LETTER_MAX){
            value = value-ASCII_LETTER_MIN+10;
        }
        return value;
    }


    @Override
    public Collection<String> doSharding(Collection<String> availableTargetNames, RangeShardingValue<String> shardingValue) {
        return availableTargetNames;
    }

    @Override
    public void init() {
        shardingCount = getShardingCount();
    }

    @Override
    public String getType() {
        return "UUID_MOD";
    }

    @Override
    public Properties getProps() {
        return props;
    }

    @Override
    public void setProps(Properties props) {
        this.props = props;
    }
}
