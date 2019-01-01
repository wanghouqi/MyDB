package hq.mydb.comparator;

import java.text.Collator;
import java.util.Comparator;
import java.util.Locale;

/**
 * Collator 类执行区分语言环境的 String 比较。使用此类可为自然语言文本构建搜索和排序例程。
 * @param <String>
 */
public class StringComparator implements Comparator<String> {

	final Collator collator;

	public StringComparator() {
		collator = Collator.getInstance();
	}

	/**
	 * Gets the Collator for the desired locale.
	 * @param desiredLocale the desired locale.
	 */
	public StringComparator(Locale desiredLocale) {
		collator = Collator.getInstance(desiredLocale);
	}

	/**
	 * The strength property determines
	 * the minimum level of difference considered significant during comparison.
	 * See the Collator class description for an example of use.
	 *
	 * @param strength Collator strength value
	 */
	public StringComparator(int strength) {
		this();
		collator.setStrength(strength);
	}

	/**
	 * @param desiredLocale the desired locale.
	 * @param strength Collator strength property value
	 */
	public StringComparator(Locale desiredLocale, int strength) {
		this(desiredLocale);
		this.collator.setStrength(strength);
	}

	@Override
	public int compare(String s1, String s2) {
		return collator.compare(s1, s2);
	}
}
