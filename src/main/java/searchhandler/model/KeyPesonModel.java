package searchhandler.model;

import searchhandler.common.utils.DateUtils;
import searchhandler.entity.BusinessInformation;
import lombok.Data;

import java.io.Serializable;

@Data
public class KeyPesonModel implements Serializable {

    private Long serialno;
    private String name;
    private String position_chinese;
    private String create_time;

    public KeyPesonModel(BusinessInformation bi) {
        this.serialno = bi.getKSerialno();
        this.name = bi.getName();
        this.position_chinese = bi.getPositionChinese();
        this.create_time = DateUtils.formatDate(bi.getKCreateTime());
    }
}
