package searchhandler.model;

import searchhandler.common.utils.DateUtils;
import searchhandler.common.utils.DataUtils;
import searchhandler.entity.BusinessInformation;
import lombok.Data;

import java.io.Serializable;
import java.math.BigDecimal;

@Data
public class ShareHolderModel implements Serializable {
    private Long serialno;
    private String shareholder_name;
    private String shareholder_type;
    private double payable_amount;
    private double payable_actual_amount;
    private String shareholder_credit_type;
    private String shareholder_credit_no;
    private String create_time;

    public ShareHolderModel(BusinessInformation bi) {
        this.serialno = bi.getSSerialno();
        this.shareholder_name = bi.getShareholderName();
        this.shareholder_type = bi.getShareholderType();
        BigDecimal payableAmount = DataUtils.handleNullValue(bi.getPayableAmount(), BigDecimal.class, new BigDecimal(0));
        this.payable_amount = payableAmount.doubleValue();
        BigDecimal payableActualAmount = DataUtils.handleNullValue(bi.getPayableActualAmount(), BigDecimal.class, new BigDecimal(0));
        this.payable_actual_amount = payableActualAmount.doubleValue();
        this.shareholder_credit_no = bi.getShareholderCreditNo();
        this.shareholder_credit_type = bi.getShareholderCreditType();
        this.create_time = DateUtils.formatDate(bi.getSCreateTime());
    }
}