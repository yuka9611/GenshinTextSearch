const formatText = (template, values) => {
  return String(template || '').replace(/\{(\w+)\}/g, (_, key) => String(values?.[key] ?? ''))
}

export default formatText
