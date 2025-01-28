// front/components/Tables/DataTable.js
import React, { useState, useEffect, useRef } from 'react';
import { View, Text, ScrollView, TextInput, TouchableOpacity } from 'react-native';
import { createPortal } from 'react-dom';
import styles from '../../styles/styles';

const DataTable = ({ data = [], title }) => {
  const [filters, setFilters] = useState({});
  const [filteredData, setFilteredData] = useState([]);
  const [error, setError] = useState(null);
  const [currentPage, setCurrentPage] = useState(1);
  const [rowsPerPage, setRowsPerPage] = useState(10);
  const rowsPerPageOptions = [5, 10, 20, 50, 100, 'Hepsi'];
  const [visibleColumns, setVisibleColumns] = useState({});
  const [showColumnSelector, setShowColumnSelector] = useState(false);
  const [columnOrder, setColumnOrder] = useState([]);
  const [draggedColumn, setDraggedColumn] = useState(null);
  const [dropdownPosition, setDropdownPosition] = useState({ top: 0, left: 0 });
  const columnSelectorRef = useRef(null);
  const [dropdownRoot, setDropdownRoot] = useState(null);
  const [activeFilterDropdown, setActiveFilterDropdown] = useState(null);

  // Filtre operatörleri
  const filterOperators = {
    text: [
      { id: 'contains', label: 'İçerir' },
      { id: 'equals', label: 'Eşittir' },
      { id: 'notEquals', label: 'Eşit Değildir' },
      { id: 'startsWith', label: 'İle Başlar' },
      { id: 'endsWith', label: 'İle Biter' },
      { id: 'empty', label: 'Boş' },
      { id: 'notEmpty', label: 'Boş Değil' }
    ],
    number: [
      { id: 'equals', label: 'Eşittir' },
      { id: 'notEquals', label: 'Eşit Değildir' },
      { id: 'greaterThan', label: 'Büyüktür' },
      { id: 'lessThan', label: 'Küçüktür' },
      { id: 'greaterThanOrEqual', label: 'Büyük Eşittir' },
      { id: 'lessThanOrEqual', label: 'Küçük Eşittir' },
      { id: 'between', label: 'Arasında' }
    ],
    date: [
      { id: 'equals', label: 'Eşittir' },
      { id: 'notEquals', label: 'Eşit Değildir' },
      { id: 'before', label: 'Önce' },
      { id: 'after', label: 'Sonra' },
      { id: 'between', label: 'Arasında' }
    ],
    boolean: [
      { id: 'equals', label: 'Eşittir' },
      { id: 'notEquals', label: 'Eşit Değildir' }
    ]
  };

  // Filtre değişikliklerini işle
  const handleFilterChange = (header, value, operator = null) => {
    setFilters(prev => ({
      ...prev,
      [header]: {
        value,
        operator: operator || prev[header]?.operator || getDefaultOperator(getColumnType(header))
      }
    }));
  };

  // Filtre operatörünü değiştir
  const handleFilterOperatorChange = (header, operator) => {
    setFilters(prev => ({
      ...prev,
      [header]: {
        value: prev[header]?.value || '',
        operator
      }
    }));
  };

  // Varsayılan operatörü al
  const getDefaultOperator = (columnType) => {
    switch (columnType) {
      case 'text': return 'contains';
      case 'number': return 'equals';
      case 'date': return 'equals';
      case 'boolean': return 'equals';
      default: return 'contains';
    }
  };

  // Filtreleme işlevi
  const applyFilters = () => {
    let result = [...data];
    
    Object.keys(filters).forEach(header => {
      const filter = filters[header];
      if (!filter || !filter.value) return;

      const columnType = getColumnType(header);
      
      result = result.filter(item => {
        const cellValue = item[header];
        const filterValue = filter.value;
        const operator = filter.operator || getDefaultOperator(columnType);
        
        // Boş değer kontrolü
        if (cellValue === null || cellValue === undefined) {
          return operator === 'empty';
        }

        switch (columnType) {
          case 'number':
            const numValue = Number(cellValue);
            const numFilterValue = Number(filterValue);
            
            if (operator === 'between') {
              const [min, max] = filterValue.split('-').map(Number);
              if (isNaN(min) || isNaN(max)) return true;
              return numValue >= min && numValue <= max;
            }
            
            if (isNaN(numValue) || isNaN(numFilterValue)) return true;
            
            switch (operator) {
              case 'equals': return numValue === numFilterValue;
              case 'notEquals': return numValue !== numFilterValue;
              case 'greaterThan': return numValue > numFilterValue;
              case 'lessThan': return numValue < numFilterValue;
              case 'greaterThanOrEqual': return numValue >= numFilterValue;
              case 'lessThanOrEqual': return numValue <= numFilterValue;
              default: return true;
            }
          
          case 'boolean':
            const boolValue = String(cellValue).toLowerCase();
            const boolFilterValue = String(filterValue).toLowerCase();
            
            switch (operator) {
              case 'equals': return boolValue === boolFilterValue;
              case 'notEquals': return boolValue !== boolFilterValue;
              default: return true;
            }

          case 'date':
            try {
              const dateValue = new Date(cellValue);
              if (isNaN(dateValue.getTime())) return false;

              if (operator === 'between') {
                const [startStr, endStr] = filterValue.split(',');
                const startDate = new Date(startStr);
                const endDate = new Date(endStr);
                
                if (isNaN(startDate.getTime()) || isNaN(endDate.getTime())) return true;
                
                // Tarih aralığı karşılaştırması için saat bilgisini sıfırla
                dateValue.setHours(0, 0, 0, 0);
                startDate.setHours(0, 0, 0, 0);
                endDate.setHours(23, 59, 59, 999);
                
                return dateValue >= startDate && dateValue <= endDate;
              }

              const filterDate = new Date(filterValue);
              if (isNaN(filterDate.getTime())) return true;

              // Tarih karşılaştırması için saat bilgisini sıfırla
              dateValue.setHours(0, 0, 0, 0);
              filterDate.setHours(0, 0, 0, 0);

              switch (operator) {
                case 'equals': return dateValue.getTime() === filterDate.getTime();
                case 'notEquals': return dateValue.getTime() !== filterDate.getTime();
                case 'before': return dateValue < filterDate;
                case 'after': return dateValue > filterDate;
                default: return true;
              }
            } catch (error) {
              console.error('Tarih filtreleme hatası:', error);
              return true;
            }
          
          default: // text
            if (!cellValue && operator === 'empty') return true;
            if (cellValue && operator === 'notEmpty') return true;

            const strValue = String(cellValue).toLowerCase();
            const strFilterValue = String(filterValue).toLowerCase();
            
            switch (operator) {
              case 'contains': return strValue.includes(strFilterValue);
              case 'equals': return strValue === strFilterValue;
              case 'notEquals': return strValue !== strFilterValue;
              case 'startsWith': return strValue.startsWith(strFilterValue);
              case 'endsWith': return strValue.endsWith(strFilterValue);
              case 'empty': return !strValue || strValue.length === 0;
              case 'notEmpty': return strValue && strValue.length > 0;
              default: return true;
            }
        }
      });
    });

    setFilteredData(result);
  };

  // Filtre bileşenini oluştur
  const renderFilter = (header) => {
    const columnType = getColumnType(header);
    const filter = filters[header] || {};
    const filterValue = filter.value || '';
    const operator = filter.operator || getDefaultOperator(columnType);
    const operators = filterOperators[columnType] || [];

    return (
      <View style={styles.filterContainer}>
        <View style={styles.filterOperatorContainer}>
          <TouchableOpacity
            style={styles.filterOperatorButton}
            onPress={() => {
              if (activeFilterDropdown === header) {
                setActiveFilterDropdown(null);
              } else {
                setActiveFilterDropdown(header);
                // Diğer açık dropdownları kapat
                if (showColumnSelector) setShowColumnSelector(false);
              }
            }}
          >
            <Text style={styles.filterOperatorText}>
              {operators.find(op => op.id === operator)?.label || 'Filtrele'}
            </Text>
          </TouchableOpacity>
          {activeFilterDropdown === header && (
            <View style={styles.filterDropdownContainer}>
              {operators.map(op => (
                <TouchableOpacity
                  key={op.id}
                  style={[
                    styles.filterDropdownItem,
                    operator === op.id && styles.filterDropdownItemActive
                  ]}
                  onPress={() => {
                    handleFilterOperatorChange(header, op.id);
                    setActiveFilterDropdown(null);
                  }}
                >
                  <Text style={styles.filterDropdownItemText}>{op.label}</Text>
                </TouchableOpacity>
              ))}
            </View>
          )}
        </View>

        {columnType === 'boolean' ? (
          <TouchableOpacity
            style={styles.filterSelect}
            onPress={() => {
              const values = ['', 'true', 'false'];
              const currentIndex = values.indexOf(filterValue);
              const nextValue = values[(currentIndex + 1) % values.length];
              handleFilterChange(header, nextValue);
            }}
          >
            <Text style={styles.filterSelectText}>
              {filterValue === 'true' ? 'Evet' : 
               filterValue === 'false' ? 'Hayır' : 'Hepsi'}
            </Text>
          </TouchableOpacity>
        ) : columnType === 'date' ? (
          operator === 'between' ? (
            <View style={{ flexDirection: 'row', gap: 4 }}>
              <input
                type="date"
                style={{
                  ...styles.filterInput,
                  flex: 1,
                }}
                value={filterValue.split(',')[0] || ''}
                onChange={(e) => {
                  const [_, endDate] = filterValue.split(',');
                  handleFilterChange(header, `${e.target.value},${endDate || ''}`);
                }}
              />
              <input
                type="date"
                style={{
                  ...styles.filterInput,
                  flex: 1,
                }}
                value={filterValue.split(',')[1] || ''}
                onChange={(e) => {
                  const [startDate] = filterValue.split(',');
                  handleFilterChange(header, `${startDate || ''},${e.target.value}`);
                }}
              />
            </View>
          ) : (
            <input
              type="date"
              style={{
                ...styles.filterInput,
                width: '100%',
              }}
              value={filterValue}
              onChange={(e) => handleFilterChange(header, e.target.value)}
            />
          )
        ) : columnType === 'number' && operator === 'between' ? (
          <TextInput
            style={styles.filterInput}
            placeholder="min-max"
            value={filterValue}
            onChangeText={(value) => {
              // Sadece sayı ve tire karakterine izin ver
              const cleanValue = value.replace(/[^0-9-]/g, '');
              handleFilterChange(header, cleanValue);
            }}
          />
        ) : (
          <TextInput
            style={styles.filterInput}
            placeholder={operator === 'between' ? "min-max" : "Ara..."}
            value={filterValue}
            onChangeText={(value) => handleFilterChange(header, value)}
          />
        )}
      </View>
    );
  };

  useEffect(() => {
    if (data.length > 0) {
      const columns = Object.keys(data[0]);
      setColumnOrder(columns);
      
      // Eğer visibleColumns boşsa, tüm sütunları görünür yap
      if (Object.keys(visibleColumns).length === 0) {
        const initialVisibility = columns.reduce((acc, column) => {
          acc[column] = true;
          return acc;
        }, {});
        setVisibleColumns(initialVisibility);
      }
    }
  }, [data]);

  const handleDragStart = (event, columnId) => {
    event.stopPropagation();
    event.dataTransfer.effectAllowed = 'move';
    setDraggedColumn(columnId);
  };

  const handleDragOver = (e, columnId) => {
    e.preventDefault();
    e.stopPropagation();
    e.dataTransfer.dropEffect = 'move';
    
    if (draggedColumn && draggedColumn !== columnId) {
      const newOrder = [...columnOrder];
      const draggedIndex = newOrder.indexOf(draggedColumn);
      const dropIndex = newOrder.indexOf(columnId);
      
      // Sütun sırasını güncelle
      newOrder.splice(draggedIndex, 1);
      newOrder.splice(dropIndex, 0, draggedColumn);
      setColumnOrder(newOrder);
    }
  };

  const handleDragEnd = (event) => {
    event.preventDefault();
    event.stopPropagation();
    setDraggedColumn(null);
  };

  useEffect(() => {
    try {
      const validData = Array.isArray(data) ? data : [];
      if (validData.length > 0 && typeof validData[0] !== 'object') {
        throw new Error('Geçersiz veri formatı');
      }
      setFilteredData(validData);
      setError(null);
    } catch (err) {
      console.error('DataTable Error:', err);
      setError('Veriler yüklenirken bir hata oluştu');
      setFilteredData([]);
    }
  }, [data]);

  // Pagination hesaplamaları
  const totalPages = Math.ceil(filteredData.length / rowsPerPage);
  const startIndex = (currentPage - 1) * rowsPerPage;
  const endIndex = startIndex + rowsPerPage;
  const currentData = filteredData.slice(startIndex, endIndex);

  const handlePageChange = (page) => {
    setCurrentPage(page);
  };

  const handleRowsPerPageChange = (value) => {
    const newValue = value === 'Hepsi' ? filteredData.length : value;
    setRowsPerPage(newValue);
    setCurrentPage(1);
  };

  // Pagination kontrollerini render et
  const renderPagination = () => (
    <View style={styles.paginationContainer}>
      <View style={styles.paginationControls}>
        <TouchableOpacity
          style={[styles.paginationButton, currentPage === 1 && styles.paginationButtonDisabled]}
          onPress={() => handlePageChange(1)}
          disabled={currentPage === 1}
        >
          <Text style={styles.paginationButtonText}>{'<<'}</Text>
        </TouchableOpacity>
        <TouchableOpacity
          style={[styles.paginationButton, currentPage === 1 && styles.paginationButtonDisabled]}
          onPress={() => handlePageChange(currentPage - 1)}
          disabled={currentPage === 1}
        >
          <Text style={styles.paginationButtonText}>{'<'}</Text>
        </TouchableOpacity>
        
        <Text style={styles.paginationText}>
          {currentPage} / {totalPages}
        </Text>
        
        <TouchableOpacity
          style={[styles.paginationButton, currentPage === totalPages && styles.paginationButtonDisabled]}
          onPress={() => handlePageChange(currentPage + 1)}
          disabled={currentPage === totalPages}
        >
          <Text style={styles.paginationButtonText}>{'>'}</Text>
        </TouchableOpacity>
        <TouchableOpacity
          style={[styles.paginationButton, currentPage === totalPages && styles.paginationButtonDisabled]}
          onPress={() => handlePageChange(totalPages)}
          disabled={currentPage === totalPages}
        >
          <Text style={styles.paginationButtonText}>{'>>'}</Text>
        </TouchableOpacity>
      </View>
    </View>
  );

  if (error) {
    return (
      <Text style={[styles.noDataText, { color: 'red' }]}>{error}</Text>
    );
  }

  if (!Array.isArray(data) || data.length === 0) {
    return (
      <Text style={styles.noDataText}>Hiç veri yok.</Text>
    );
  }

  // Veri tipini belirle
  const getColumnType = (header) => {
    const value = data[0][header];
    if (typeof value === 'number') return 'number';
    if (typeof value === 'boolean') return 'boolean';
    if (value instanceof Date || (typeof value === 'string' && !isNaN(Date.parse(value)))) return 'date';
    return 'text';
  };

  const formatDate = (dateString) => {
    if (!dateString) return '';
    const date = new Date(dateString);
    return date.toLocaleDateString('tr-TR', {
      year: 'numeric',
      month: '2-digit',
      day: '2-digit',
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit'
    });
  };

  const renderCell = (value, columnId) => {
    const formatValue = (val) => {
      if (val === null || val === undefined) return '';
      const strValue = String(val);
      if (strValue.length > 16) {
        return strValue.substring(0, 16) + '...';
      }
      return strValue;
    };

    const displayValue = typeof value === 'string' && value.includes('T') && value.includes('Z') 
      ? formatDate(value)
      : formatValue(value);

    return (
      <div style={{ position: 'relative', width: '100%' }}>
        <div 
          style={{ 
            width: '100%',
            minWidth: '200px',
            overflow: 'hidden',
            textOverflow: 'ellipsis',
            whiteSpace: 'nowrap',
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'flex-start',
            paddingLeft: '16px'
          }}
        >
          <Text 
            style={{
              ...styles.tableCellText,
              textAlign: 'left',
              width: 'auto'
            }}
            data-tooltip={String(value)}
            onMouseEnter={(e) => {
              const tooltip = document.createElement('div');
              tooltip.className = 'tooltip';
              tooltip.textContent = String(value);
              tooltip.style.position = 'fixed';
              const rect = e.currentTarget.getBoundingClientRect();
              tooltip.style.left = `${rect.left}px`;
              tooltip.style.top = `${rect.top - 40}px`;
              tooltip.style.backgroundColor = '#1a1f25';
              tooltip.style.color = '#fff';
              tooltip.style.paddingTop = '8px';
              tooltip.style.paddingBottom = '8px';
              tooltip.style.paddingLeft = '8px';
              tooltip.style.paddingRight = '8px';
              tooltip.style.borderRadius = '4px';
              tooltip.style.zIndex = '10000';
              tooltip.style.whiteSpace = 'normal';
              tooltip.style.maxWidth = '300px';
              tooltip.style.wordBreak = 'break-word';
              tooltip.style.borderStyle = 'solid';
              tooltip.style.borderWidth = '1px';
              tooltip.style.borderColor = 'rgba(255, 255, 255, 0.1)';
              tooltip.style.boxShadow = '0 4px 6px rgba(0, 0, 0, 0.1)';
              document.body.appendChild(tooltip);
            }}
            onMouseLeave={(e) => {
              const tooltip = document.querySelector('.tooltip');
              if (tooltip) {
                tooltip.remove();
              }
            }}
          >
            {displayValue}
          </Text>
        </div>
      </div>
    );
  };

  useEffect(() => {
    applyFilters();
  }, [filters]);

  // Dropdown pozisyonunu hesapla
  const calculateDropdownPosition = (buttonElement) => {
    if (!buttonElement) return;
    
    const rect = buttonElement.getBoundingClientRect();
    const spaceBelow = window.innerHeight - rect.bottom;
    const spaceAbove = rect.top;
    const dropdownHeight = 400; // Maksimum dropdown yüksekliği
    
    let top = rect.bottom;
    if (spaceBelow < dropdownHeight && spaceAbove > spaceBelow) {
      // Eğer aşağıda yeterli alan yoksa ve yukarıda daha fazla alan varsa yukarı aç
      top = rect.top - dropdownHeight;
    }

    setDropdownPosition({
      top: top,
      left: rect.left,
    });
  };

  // Sütun görünürlüğünü değiştir
  const toggleColumn = (columnId, event) => {
    event.stopPropagation();
    setVisibleColumns(prev => {
      const newState = { ...prev };
      newState[columnId] = !newState[columnId];
      
      // En az bir sütun görünür olmalı
      const visibleCount = Object.values(newState).filter(Boolean).length;
      if (visibleCount === 0) {
        return prev;
      }
      
      return newState;
    });
  };

  // Dropdown dışına tıklandığında kapat
  useEffect(() => {
    const handleClickOutside = (event) => {
      if (columnSelectorRef.current && !columnSelectorRef.current.contains(event.target)) {
        setShowColumnSelector(false);
      }
    };

    if (showColumnSelector) {
      document.addEventListener('mousedown', handleClickOutside);
    }

    return () => {
      document.removeEventListener('mousedown', handleClickOutside);
    };
  }, [showColumnSelector]);

  // Sütun seçici butonunu render et
  const renderColumnSelector = () => {
    return (
      <div 
        ref={columnSelectorRef}
        style={styles.columnSelector}
      >
        <div 
          style={styles.columnSelectorButton}
          onClick={(e) => {
            e.stopPropagation();
            setShowColumnSelector(!showColumnSelector);
            setActiveFilterDropdown(null);
          }}
        >
          <span style={styles.columnSelectorButtonText}>
            Sütunları Göster/Gizle
          </span>
        </div>
        {showColumnSelector && (
          <div style={styles.columnSelectorDropdown}>
            {columnOrder.map(columnId => (
              <div
                key={columnId}
                style={styles.columnSelectorItem}
                onClick={(e) => toggleColumn(columnId, e)}
                draggable
                onDragStart={(e) => handleDragStart(e, columnId)}
                onDragOver={(e) => handleDragOver(e, columnId)}
                onDragEnd={handleDragEnd}
              >
                <div style={{
                  ...styles.checkbox,
                  ...(visibleColumns[columnId] !== false ? styles.checkboxChecked : {})
                }}>
                  {visibleColumns[columnId] !== false && "✓"}
                </div>
                <span style={styles.columnSelectorItemText}>
                  {columnId}
                </span>
              </div>
            ))}
          </div>
        )}
      </div>
    );
  };

  // Görünür sütunları hesapla
  const visibleHeaders = columnOrder.filter(column => visibleColumns[column] !== false);

  // Tablo başlığını render et
  const renderTableHeader = () => (
    <div style={{ ...styles.tableHeader, width: `${visibleHeaders.length * 200}px` }}>
      {visibleHeaders.map((header, index) => (
        <div 
          key={header} 
          style={{
            ...styles.headerCell,
            minWidth: '200px',
            flex: 1,
            ...(index === visibleHeaders.length - 1 && { borderRightWidth: 0 }),
            ...(draggedColumn === header && { opacity: 0.5 }),
          }}
          draggable={true}
          onDragStart={(e) => handleDragStart(e, header)}
          onDragOver={(e) => handleDragOver(e, header)}
          onDragEnd={handleDragEnd}
          onDrop={(e) => handleDragEnd(e)}
        >
          <div style={{ 
            display: 'flex', 
            alignItems: 'center', 
            justifyContent: 'center',
            cursor: 'grab',
            width: '100%',
            height: '100%',
            userSelect: 'none'
          }}>
            <Text style={styles.tableHeaderCell}>{header}</Text>
          </div>
          {renderFilter(header)}
        </div>
      ))}
    </div>
  );

  return (
    <div style={styles.mainContainer}>
      <div style={styles.tableControls}>
        {renderColumnSelector()}
        <div style={styles.tableControlsRight}>
          <View style={styles.rowsPerPageContainer}>
            <Text style={styles.paginationText}>Sayfa başına satır:</Text>
            <View style={styles.rowsPerPageSelect}>
              {rowsPerPageOptions.map((option) => (
                <TouchableOpacity
                  key={option}
                  style={[
                    styles.rowsPerPageOption,
                    (option === 'Hepsi' ? rowsPerPage === filteredData.length : rowsPerPage === option) && styles.rowsPerPageOptionActive
                  ]}
                  onPress={() => handleRowsPerPageChange(option)}
                >
                  <Text style={[
                    styles.rowsPerPageOptionText,
                    (option === 'Hepsi' ? rowsPerPage === filteredData.length : rowsPerPage === option) && styles.rowsPerPageOptionTextActive
                  ]}>
                    {option}
                  </Text>
                </TouchableOpacity>
              ))}
            </View>
          </View>
          <View style={styles.paginationControls}>
            <TouchableOpacity
              style={[styles.paginationButton, currentPage === 1 && styles.paginationButtonDisabled]}
              onPress={() => handlePageChange(1)}
              disabled={currentPage === 1}
            >
              <Text style={styles.paginationButtonText}>{'<<'}</Text>
            </TouchableOpacity>
            <TouchableOpacity
              style={[styles.paginationButton, currentPage === 1 && styles.paginationButtonDisabled]}
              onPress={() => handlePageChange(currentPage - 1)}
              disabled={currentPage === 1}
            >
              <Text style={styles.paginationButtonText}>{'<'}</Text>
            </TouchableOpacity>
            
            <Text style={styles.paginationText}>
              {currentPage} / {totalPages}
            </Text>
            
            <TouchableOpacity
              style={[styles.paginationButton, currentPage === totalPages && styles.paginationButtonDisabled]}
              onPress={() => handlePageChange(currentPage + 1)}
              disabled={currentPage === totalPages}
            >
              <Text style={styles.paginationButtonText}>{'>'}</Text>
            </TouchableOpacity>
            <TouchableOpacity
              style={[styles.paginationButton, currentPage === totalPages && styles.paginationButtonDisabled]}
              onPress={() => handlePageChange(totalPages)}
              disabled={currentPage === totalPages}
            >
              <Text style={styles.paginationButtonText}>{'>>'}</Text>
            </TouchableOpacity>
          </View>
        </div>
      </div>
      <div style={styles.tableWrapper}>
        {renderTableHeader()}
        <div style={{ ...styles.tableBody, width: `${visibleHeaders.length * 200}px` }}>
          {currentData.map((row, rowIndex) => (
            <div key={rowIndex} style={{
              ...styles.tableRow,
              ...(rowIndex % 2 === 0 ? styles.tableRowEven : styles.tableRowOdd),
              width: `${visibleHeaders.length * 200}px`,
              ...(rowIndex === currentData.length - 1 && { borderBottomWidth: 0 })
            }}>
              {visibleHeaders.map((header, colIndex) => (
                <div 
                  key={header} 
                  style={{
                    ...styles.tableCell,
                    minWidth: '200px',
                    flex: 1,
                    ...(colIndex === visibleHeaders.length - 1 && { borderRightWidth: 0 })
                  }}
                >
                  {renderCell(row[header], header)}
                </div>
              ))}
            </div>
          ))}
        </div>
      </div>
    </div>
  );
};

export default DataTable;
