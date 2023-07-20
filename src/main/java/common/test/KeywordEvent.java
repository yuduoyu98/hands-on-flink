package common.test;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
//POJO必须要有无参构造器
@NoArgsConstructor
public class KeywordEvent {

    public String word;
    public Long cnt;
    public Long ts;

}
