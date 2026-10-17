package org.hgc.suts.picture.mq.event;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;


/**
 * 上传图片分析事件
 */

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class UploadPictureAnalysisEvent {

    /**
     * 用户id
     */
    private Long userId;
    /** Capture/request time, preserved across MQ retries so stale analyses cannot overwrite newer ones. */
    private Long requestedAt;

    /**
     * 图片 ID
     */
    private Long pictureId;

    /**
     * 图片在 COS 中的 Key
     */
    private String imageKey;

    /**
     * 用户描述内容
     */
    String descriptionContent;
}
