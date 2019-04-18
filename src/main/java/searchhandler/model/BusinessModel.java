package searchhandler.model;

import searchhandler.common.utils.DateUtils;
import searchhandler.entity.BusinessInformation;
import lombok.Data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

@Data
public class BusinessModel implements Serializable {

    private String province_code;
    private String province_name;
    private Long serialno = 0L;
    private Long correlation_no;
    private String registration_no;
    private String credit_code;
    private String enterprises_type;
    private String registered_capital_origin;
    private String business_time_start;
    private String registration_authority;
    private String registration_status;
    private String residence;
    private String business_scope;
    private String enterprise_name;
    private String charge_person;
    private String establishment_date;
    private String business_time_end;
    private String approval_date;
    private String create_time;
    private String spider_time;
    private List<KeyPesonModel> key_personnel = new ArrayList<>();
    private List<ShareHolderModel> shareholder_information = new ArrayList<>();

    public BusinessModel(String provinceCode, String provinceName) {
        super();
        this.province_code = provinceCode;
        this.province_name = provinceName;
    }

    public BusinessModel initEssentialData(BusinessInformation bi) {
        this.serialno = bi.getSerialno();
        this.correlation_no = bi.getCorrelationNo();
        this.registration_no = bi.getRegistrationNo();
        this.credit_code = bi.getCreditCode();
        this.enterprises_type = bi.getEnterprisesType();
        this.registered_capital_origin = bi.getRegisteredCapitalOrigin();
        this.business_time_start = DateUtils.formatDate(bi.getBusinessTimeStart());
        this.registration_authority = bi.getRegistrationAuthority();
        this.registration_status = bi.getRegistrationStatus();
        this.residence = bi.getResidence();
        this.enterprise_name = bi.getEnterpriseName();
        this.charge_person = bi.getChargePerson();
        this.business_scope = bi.getBusinessScope();
        this.establishment_date = DateUtils.formatDate(bi.getEstablishmentDate());
        this.business_time_end = DateUtils.formatDate(bi.getBusinessTimeEnd());
        this.approval_date = DateUtils.formatDate(bi.getApprovalDate());
        this.create_time = DateUtils.formatDate(bi.getCreateTime());
        this.spider_time = DateUtils.formatDate(bi.getSpiderTime());
        return this;
    }
}
