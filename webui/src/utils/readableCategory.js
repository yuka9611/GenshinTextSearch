export const READABLE_CATEGORY_LABELS = Object.freeze({
  BOOK: '书籍',
  ITEM: '道具',
  READABLE: '阅读物',
  COSTUME: '角色装扮',
  RELIC: '圣遗物',
  WEAPON: '武器',
  WINGS: '风之翼',
})

export const READABLE_CATEGORY_OPTIONS = Object.freeze([
  { value: '', label: '全部' },
  { value: 'BOOK', label: READABLE_CATEGORY_LABELS.BOOK },
  { value: 'ITEM', label: READABLE_CATEGORY_LABELS.ITEM },
  { value: 'READABLE', label: READABLE_CATEGORY_LABELS.READABLE },
  { value: 'COSTUME', label: READABLE_CATEGORY_LABELS.COSTUME },
  { value: 'RELIC', label: READABLE_CATEGORY_LABELS.RELIC },
  { value: 'WEAPON', label: READABLE_CATEGORY_LABELS.WEAPON },
  { value: 'WINGS', label: READABLE_CATEGORY_LABELS.WINGS },
])

export const getReadableCategoryLabel = (category) => {
  const normalized = String(category || '').trim().toUpperCase()
  return READABLE_CATEGORY_LABELS[normalized] || ''
}
