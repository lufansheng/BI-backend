package com.yupi.springbootinit.bizmq;

import com.alibaba.excel.util.StringUtils;
import com.rabbitmq.client.Channel;
import com.yupi.springbootinit.common.ErrorCode;
import com.yupi.springbootinit.exception.BusinessException;
import com.yupi.springbootinit.manager.AiManager;
import com.yupi.springbootinit.model.entity.Chart;
import com.yupi.springbootinit.service.ChartService;
import com.yupi.springbootinit.utils.ExcelUtils;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;

import static com.yupi.springbootinit.constant.CommonConstant.BI_MODEL_ID;

@Component
@Slf4j
public class BiMessageConsumer {

    @Resource
    private ChartService chartService;

    @Resource
    private AiManager aiManager;

    @SneakyThrows
    @RabbitListener(queues = {BiMqConstant.BI_QUEUE_NAME},ackMode = "MANUAL")
    public void receiveMessage(String message, Channel channel, @Header(AmqpHeaders.DELIVERY_TAG) long delicertTag){
        log.info("receiveMessage = {}",message);
        if (StringUtils.isBlank(message)){
            channel.basicNack(delicertTag,false,false);
            throw new BusinessException(ErrorCode.SYSTEM_ERROR,"消息为空");
        }
        long chartId = Long.parseLong(message);
        Chart chart = chartService.getById(chartId);
        if (chart == null){
            channel.basicNack(delicertTag,false,false);
            throw new BusinessException(ErrorCode.NOT_FOUND_ERROR,"图表为空");
        }
        Chart updateChart = new Chart();
        updateChart.setId(chart.getId());
        updateChart.setStatus("running");
        boolean b = chartService.updateById(updateChart);
        if (!b){
            //todo:数据库里改成失败
            channel.basicNack(delicertTag,false,false);

            handleChartUpdateErroe(chart.getId(),"更新图表执行中状态失败");
            return;
        }
        //拿到返回结果
        String result = aiManager.doChat(BI_MODEL_ID, buildUserInput(chart));

        String[] split = result.split("【【【【【");
        if (split.length < 3) {
            channel.basicNack(delicertTag,false,false);

            handleChartUpdateErroe(chart.getId(), "AI生产错误");
        }

        String genChart = split[1].trim();
        String genResult = split[2].trim();

        //调用AI结果后再更新一次
        Chart updateChartResult = new Chart();
        updateChartResult.setId(chart.getId());
        updateChartResult.setGenChart(genChart);
        updateChartResult.setGenResult(genResult);
        updateChartResult.setStatus("succeed");

        boolean updateResult = chartService.updateById(updateChartResult);
        System.out.println(updateChartResult.getStatus());
        if (!updateResult){
            channel.basicNack(delicertTag,false,false);

            handleChartUpdateErroe(chart.getId(),"更新图表成功状态失败");
            return;
        }

        log.info("receiceMessage message = {}",message);
        channel.basicAck(delicertTag,false);

    }

    private void handleChartUpdateErroe(long chartId,String execMessage){
        Chart updateChartResult = new Chart();
        updateChartResult.setId(chartId);
        updateChartResult.setStatus("failed");
        updateChartResult.setExecMessage(execMessage);
        boolean updateResult = chartService.updateById(updateChartResult);
        if (!updateResult){
            log.error("更新图表失败状态失败" + chartId + "," + execMessage);
        }
    }

    private String buildUserInput(Chart chart){

        String goal = chart.getGoal();
        String chartType = chart.getChartType();
        String csvData = chart.getChartData();

        // 构造用户输入
        StringBuilder userInput = new StringBuilder();
        userInput.append("分析需求：").append("\n");

// 拼接分析目标
        String userGoal = goal;
        if (StringUtils.isNotBlank(chartType)) {
            userGoal += "，请使用" + chartType;
        }
        userInput.append(userGoal).append("\n");
        userInput.append("原始数据：").append("\n");
// 压缩后的数据

        userInput.append(csvData).append("\n");
        return userInput.toString();
    }
}
