package io.openmessaging.storage.dledger;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class SimpleTest {

    @Test
    public void testOne() {
        Map<String, Long> peerWaterMarks = new HashMap<>();
        peerWaterMarks.put("dledger_group_01_1", 100L);
        peerWaterMarks.put("dledger_group_01_1", 100L);
        peerWaterMarks.put("dledger_group_01_0", 101L);
        long quorumIndex = -1;
        for (Long index : peerWaterMarks.values()) {
            int num = 0;
            // 遍历 peerWaterMarks 中的所有已提交序号，
            // 与当前值进行比较，如果节点的已提交序号大于等于待投票的日志序号(index)，num 加一
            // 表示投赞成票
            for (Long another : peerWaterMarks.values()) {
                if (another >= index) {
                    num++;
                }
            }
            // 对 index 进行仲裁，如果超过半数 并且 index 大于 quorumIndex，
            // 更新 quorumIndex 的值为 index。
            // quorumIndex 经过遍历的，得出当前最大的可提交日志序号。
            if (num >= 2 && index > quorumIndex) {
                quorumIndex = index;
            }
        }
        System.out.println(quorumIndex);
    }
}
