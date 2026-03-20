# -*- coding: utf-8 -*-
"""
SEC EDGAR Parser 抽象基类
========================
所有表单解析器 (Form 4, 13F, SC 13D, 8-K, 10-Q, 10-K) 继承此类。
提供 XML 解析工具函数和统一的接口契约。
"""

import xml.etree.ElementTree as ET
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional
from src.utils.logger import app_logger


class BaseEdgarParser(ABC):
    """SEC EDGAR 表单解析器基类"""

    # 子类必须覆盖
    FORM_TYPE: str = ""

    @abstractmethod
    def parse(self, xml_text: str, metadata: Optional[Dict[str, str]] = None) -> List[Dict[str, Any]]:
        """
        解析单份申报文档,返回扁平化的字典列表 (每行对应一条记录)。
        metadata: 额外元信息 (如 filingDate, accessionNumber)
        """
        ...

    # ──────────────────────────────────────────────
    # XML 工具函数
    # ──────────────────────────────────────────────
    @staticmethod
    def safe_parse_xml(xml_text: str) -> Optional[ET.Element]:
        """安全解析 XML 文档, 自动处理 namespace 和编码异常"""
        try:
            # 去除常见的 XML namespace 前缀干扰
            cleaned = xml_text
            if "xmlns=" in cleaned:
                import re
                cleaned = re.sub(r'\sxmlns="[^"]*"', "", cleaned, count=1)
            return ET.fromstring(cleaned)
        except ET.ParseError as e:
            app_logger.warning(f"XML 解析失败: {e}")
            return None

    @staticmethod
    def get_text(element: Optional[ET.Element], path: str, default: str = "") -> str:
        """安全地从 XML 元素中获取文本内容"""
        if element is None:
            return default
        node = element.find(path)
        if node is not None and node.text:
            return node.text.strip()
        # 尝试在子节点中找 <value> 标签 (SEC 常见模式)
        value_node = element.find(f"{path}/value")
        if value_node is not None and value_node.text:
            return value_node.text.strip()
        return default

    @staticmethod
    def get_float(element: Optional[ET.Element], path: str, default: float = 0.0) -> float:
        """安全地从 XML 元素中获取浮点数"""
        text = BaseEdgarParser.get_text(element, path)
        if not text:
            return default
        try:
            return float(text.replace(",", ""))
        except (ValueError, TypeError):
            return default

    @staticmethod
    def get_int(element: Optional[ET.Element], path: str, default: int = 0) -> int:
        """安全地从 XML 元素中获取整数"""
        text = BaseEdgarParser.get_text(element, path)
        if not text:
            return default
        try:
            return int(float(text.replace(",", "")))
        except (ValueError, TypeError):
            return default
