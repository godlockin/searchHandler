package com.tigerobo.searchhandler.model;

import com.tigerobo.searchhandler.common.utils.DataUtils;
import com.tigerobo.searchhandler.entity.BusinessInformation;
import lombok.Data;

import java.io.Serializable;

@Data
public class KeyPesonModel implements Serializable {

    private Integer serialno;
    private String name;
    private String position_chinese;
    private String create_time;

    public KeyPesonModel(BusinessInformation bi) {
        this.serialno = bi.getKSerialno();
        this.name = bi.getName();
        this.position_chinese = bi.getPositionChinese();
        this.create_time = DataUtils.formatDate(bi.getKCreateTime());
    }
}
