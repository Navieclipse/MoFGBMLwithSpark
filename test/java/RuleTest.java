import static org.junit.Assert.*;

import org.junit.Test;

import gbml.Rule;

public class RuleTest {

	@Test
	public void testGetRuleNum() {

		Rule rule = new Rule();
		int Ndim = 4;
		rule.setMic(Ndim);
		for(int i=0; i<Ndim; i++){
			rule.setRule(i, 1);
		}
		rule.setLength( rule.ruleLengthCalc() );
		assertEquals( rule.getRuleLength() , Ndim );
	}


}
