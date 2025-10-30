package sunyu.util;

import cn.hutool.core.util.NumberUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;

/**
 * 根据输入数字，转换比较运算符的正则表达式
 *
 * @author 孙宇
 *
 * <p>
 * 例：
 * < 55.06   正则表达式为：^-\d+(?:|\.\d+)$|^(?:|[0-4])\d{1,1}(?:|\.\d+)$|^5[0-4](?:|\.\d+)$|^55(?:|\.[0]+)$|^55\.0(?:|[0-5]\d*)$
 * > -8.3    正则表达式为：^\d+(?:|\.\d+)$|^-(?:|[0-7])(?:|\.\d+)$|^-8(?:|\.[0-2]\d*)$
 * >  18     正则表达式为：^18\.\d*[1-9]+\d*$|^[1-9]\d{2,}(?:|\.\d+)$|^[2-9]\d{1}(?:|\.\d+)$|^1[9-9](?:|\.\d+)$
 * < -13     正则表达式为：^-[1-9]\d{2,}(?:|\.\d+)$|^-[2-9]\d{1}(?:|\.\d+)$|^-1[4-9](?:|\.\d+)$|^-13\.\d*[1-9]+\d*$
 * <p>
 * 整体思路：
 * 针对全体非负数：
 * 小于当前数字所有位数的数字；从高位向低位遍历，遇到0特殊处理；截取前面数字，和小于当前位数字，后面是任意数字，总体长度要和给定数字相等
 * 大于当前数字所有位数的数字；从高位向低位遍历，遇到9特殊处理；截取前面数字，和大于当前位数字，后面是任意数字，总体长度要和给定数字相等
 * 当针对小数时，要多大考虑小数后面的非零数字的特性。
 * 当遇到负数时，转换为正数进行计算。
 */
public class RegexUtil implements AutoCloseable {
    private final Log log = LogFactory.get();
    @SuppressWarnings("unused")
    private Config config;

    public static Builder builder() {
        return new Builder();
    }

    private RegexUtil(Config config) {
        log.info("[构建{}] 开始", this.getClass().getSimpleName());
        log.info("[构建{}] 结束", this.getClass().getSimpleName());
        this.config = config;
    }

    private static class Config {
    }

    public static class Builder {
        private final Config config = new Config();

        public RegexUtil build() {
            return new RegexUtil(config);
        }
    }

    /**
     * 回收资源
     */
    @Override
    public void close() {
        log.info("[销毁{}] 开始", this.getClass().getSimpleName());
        log.info("[销毁{}] 结束", this.getClass().getSimpleName());
    }

    /**
     * 小于指定数字的正则表达式
     *
     * @param s
     * @return
     */
    public String transformLessNumber(String s) {
        StringBuilder sb = new StringBuilder();
        if (NumberUtil.parseNumber(s).doubleValue() < 0) {
            //负数
            s = s.substring(1);
            String[] split = delZero(s.split("\\."));
            if (split.length == 1) {
                //整数
                transformGreaterInteger("-", split[0], sb);
                sb.append("|^-").append(split[0]).append("\\.\\d*[1-9]+\\d*$");
            } else if (split.length == 2) {
                //小数
                transformGreaterDouble("-", split, sb);
                sb.append("|^-").append(split[0]).append("\\.").append(split[1]).append("\\d*[1-9]+\\d*$");
            }
        } else {
            //正数
            String[] split = delZero(s.split("\\."));
            //添加负数
            sb.append("^-\\d+(?:|\\.\\d+)$");
            if (split.length == 1) {
                //整数
                transformLessInteger("", split[0], sb);
            } else if (split.length == 2) {
                //小数
                transformLessDouble("", split, sb);
            }
        }
        return sb.toString();
    }

    /**
     * 小于等于指定数字的正则表达式
     *
     * @param s
     * @return
     */
    public String transformLessOrEqualNumber(String s) {
        StringBuilder sb = new StringBuilder();
        if (NumberUtil.parseNumber(s).doubleValue() < 0) {
            //负数
            s = s.substring(1);
            String[] split = delZero(s.split("\\."));
            if (split.length == 1) {
                //整数
                sb.append("^-").append(split[0]).append("(?:|\\.\\d+)$|");
                transformGreaterInteger("-", split[0], sb);
            } else if (split.length == 2) {
                //小数
                sb.append("^-").append(split[0]).append("\\.").append(split[1]).append("\\d*$|");
                transformGreaterDouble("-", split, sb);
            }
        } else {
            //正数
            String[] split = delZero(s.split("\\."));
            //添加负数
            sb.append("^-\\d+(?:|\\.\\d+)$");
            if (split.length == 1) {
                //整数
                transformLessInteger("", split[0], sb);
                sb.append("|^").append(split[0]).append("(?:|\\.\\d+)$");
            } else if (split.length == 2) {
                //小数
                transformLessDouble("", split, sb);
                sb.append("|^").append(split[0]).append("\\.").append(split[1]).append("\\d*$");
            }
        }
        return sb.toString();
    }

    /**
     * 大于指定数字的正则表达式
     *
     * @param s
     * @return
     */
    public String transformGreaterNumber(String s) {
        StringBuilder sb = new StringBuilder();
        if (NumberUtil.parseNumber(s).doubleValue() < 0) {
            //负数
            s = s.substring(1);
            String[] split = delZero(s.split("\\."));
            //添加所有正数
            sb.append("^\\d+(?:|\\.\\d+)$");
            if (split.length == 1) {
                //整数
                transformLessInteger("-", split[0], sb);
            } else if (split.length == 2) {
                //小数
                transformLessDouble("-", split, sb);
            }
        } else {
            //正数
            String[] split = delZero(s.split("\\."));
            if (split.length == 1) {
                //整数
                sb.append("^").append(split[0]).append("\\.\\d*[1-9]+\\d*$|");
                transformGreaterInteger("", split[0], sb);
                //添加小数大于整数部分
            } else if (split.length == 2) {
                //小数
                sb.append("^").append(split[0]).append("\\.").append(split[1]).append("\\d*[1-9]+\\d*$|");
                transformGreaterDouble("", split, sb);
            }
        }
        return sb.toString();
    }

    /**
     * 大于或等于指定数字的正则表达式
     *
     * @param s
     * @return
     */
    public String transformGreaterOrEqualNumber(String s) {
        StringBuilder sb = new StringBuilder();
        if (NumberUtil.parseNumber(s).doubleValue() < 0) {
            //负数
            s = s.substring(1);
            String[] split = delZero(s.split("\\."));
            //添加所有正数
            sb.append("^\\d+(?:|\\.\\d+)$");
            if (split.length == 1) {
                //整数
                transformLessInteger("-", split[0], sb);
                sb.append("|^-").append(split[0]).append("(?:|\\.\\d+)$");
            } else if (split.length == 2) {
                //小数
                transformLessDouble("-", split, sb);
                sb.append("|^-").append(split[0]).append("\\.").append(split[1]).append("\\d*$");
            }
        } else {
            //正数
            String[] split = delZero(s.split("\\."));
            if (split.length == 1) {
                //整数
                sb.append("^").append(split[0]).append("(?:|\\.\\d+)$|");
                transformGreaterInteger("", split[0], sb);
                //添加小数大于整数部分
            } else if (split.length == 2) {
                //小数
                sb.append("^").append(split[0]).append("\\.").append(split[1]).append("\\d*$|");
                transformGreaterDouble("", split, sb);
            }
        }
        return sb.toString();
    }

    /**
     * 等于指定数字的正则表达式
     *
     * @param s
     * @return
     */
    public String transformEqualNumber(String s) {
        String[] split = delZero(s.split("\\."));
        StringBuilder sb = new StringBuilder();
        if (split.length == 1) {
            //整数
            sb.append("^").append(split[0]).append("(?:|\\.[0]+)$");
        } else if (split.length == 2) {
            //小数
            sb.append("^").append(split[0]).append("\\.").append(split[1]).append("[0]*$");
        }
        return sb.toString();
    }

    /**
     * 转换大于整数的正则表达式
     *
     * @param sign
     * @param s
     * @param sb
     */
    private void transformGreaterInteger(String sign, String s, StringBuilder sb) {
        //先添加高于给定数字位数的条件
        sb.append("^").append(sign).append("[1-9]\\d{").append(s.length()).append(",}(?:|\\.\\d+)$");
        for (int i = 0; i < s.length(); i++) {
            int n = Integer.parseInt(s.substring(i, i + 1));
            if (n != 9) {
                String pre = s.substring(0, i);
                int num = s.length() - 1 - i;
                sb.append("|^").append(sign).append(pre).append("[").append(n + 1);
                if (num == 0) {
                    sb.append("-9]").append("(?:|\\.\\d+)$");
                } else {
                    sb.append("-9]\\d{").append(num).append("}(?:|\\.\\d+)$");
                }
            }
        }
    }

    /**
     * 转换大于浮点类型数字的正则表达式
     *
     * @param sign
     * @param split
     * @param sb
     */
    private void transformGreaterDouble(String sign, String[] split, StringBuilder sb) {
        String s1 = split[0];
        //先转换整数部分
        transformGreaterInteger(sign, s1, sb);
        //在转换小数部分
        String s2 = split[1];
        for (int i = 0; i < s2.length(); i++) {
            int n = Integer.parseInt(s2.substring(i, i + 1));
            if (n != 9) {
                String pre = s2.substring(0, i);
                sb.append("|^").append(sign).append(s1).append("\\.").append(pre).append("[").append(n + 1)
                        .append("-9]\\d*$");
            }
        }
    }

    /**
     * 转换小于整数的正则表达式
     *
     * @param sign
     * @param s
     * @param sb
     */
    private void transformLessInteger(String sign, String s, StringBuilder sb) {
        for (int i = 0; i < s.length(); i++) {
            int n = Integer.parseInt(s.substring(i, i + 1));
            if (n == 0) {
                continue;
            }
            sb.append("|^").append(sign);
            if (i > 0) {
                String pre = s.substring(0, i);
                sb.append(pre).append("[0-").append(n - 1).append("]");
            } else {
                if (n == 1) {
                    sb.append("(?:|[0])");
                } else {
                    sb.append("(?:|[0-").append(n - 1).append("])");
                }
            }
            int num = s.length() - i - 1;
            if (num == 0) {
                sb.append("(?:|\\.\\d+)$");
            } else {
                sb.append("\\d{1,").append(num).append("}(?:|\\.\\d+)$");
            }
        }
    }

    /**
     * 转换小于浮点类型数字的正则表达式
     *
     * @param sign
     * @param split
     * @param sb
     */
    private void transformLessDouble(String sign, String[] split, StringBuilder sb) {
        String s1 = split[0];
        //先转换整数部分
        transformLessInteger(sign, s1, sb);
        //在转换小数部分
        String s2 = split[1];
        for (int i = 0; i < s2.length(); i++) {
            int n = Integer.parseInt(s2.substring(i, i + 1));
            if (i > 0) {
                String pre = s2.substring(0, i);
                sb.append("|^").append(sign).append(s1).append("\\.").append(pre);
                if (n != 0) {
                    sb.append("(?:|[0-").append(n - 1).append("]\\d*)$");
                } else {
                    sb.append("[0]*$");
                }
            } else {
                sb.append("|^").append(sign).append(s1);
                if (n == 0) {
                    sb.append("(?:|\\.[0]+)$");
                } else {
                    sb.append("(?:|\\.[0-").append(n - 1).append("]\\d*)$");
                }
            }
        }
    }

    /**
     * 去除数字小数点后，无效的0
     *
     * @param split
     * @return
     */
    private String[] delZero(String[] split) {
        if (split.length == 1) {
            return split;
        } else if (split.length == 2) {
            String s = split[1];
            int i = Integer.parseInt(StrUtil.reverse(s));
            if (i == 0) {
                return new String[] { split[0] };
            }
            split[1] = StrUtil.reverse(String.valueOf(i));
            return split;
        }
        return null;
    }

}